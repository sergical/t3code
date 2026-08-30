import {
  classifyTaskAgentKind,
  isToolLifecycleItemType,
  type OrchestrationEvent,
  type OrchestrationTraceContext,
  type ProviderRuntimeEvent,
  type RuntimeItemId,
  type RuntimeTaskId,
  type RuntimeTaskUsage,
  type ThreadId,
  type ThreadTokenUsageSnapshot,
  type TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { forkParked } from "../../serverActivation.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import {
  ProviderTurnTracing,
  type ProviderTurnTracingShape,
} from "../Services/ProviderTurnTracing.ts";

const GEN_AI_SYSTEM: Record<string, string> = {
  claudeAgent: "anthropic",
  codex: "openai",
  grok: "xai",
};

const encodeFinishReasons = Schema.encodeSync(Schema.fromJsonString(Schema.Array(Schema.String)));

// Sentry applies no size cap of its own; a single tool result can be megabytes.
const MAX_CONTENT_CHARS = 16_000;

const clip = (text: string) =>
  text.length > MAX_CONTENT_CHARS ? `${text.slice(0, MAX_CONTENT_CHARS)}…[truncated]` : text;

const encodeJson = (value: unknown) => {
  try {
    return clip(JSON.stringify(value));
  } catch {
    return String(value);
  }
};

const encodeMessages = (role: "user" | "assistant", content: string) =>
  encodeJson([{ role, content }]);

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null;

// Item `data` is the adapter's own shape. Claude carries {toolName, input, result};
// Codex the raw provider payload. Split the Claude shape so the output attribute
// does not repeat the input, and fall back to the whole payload otherwise.
const toolInput = (data: unknown) => (isRecord(data) && "input" in data ? data.input : data);
const toolOutput = (data: unknown) => (isRecord(data) && "result" in data ? data.result : data);

interface StepUsage {
  input: number;
  output: number;
  cached: number | undefined;
  reasoning: number | undefined;
}

// Codex reports the last response in the last* fields; Claude only fills the plain
// ones, and only on the `message_delta` that closes a model response. Snapshots
// without `inputTokens` (task progress, compaction) are context-window bookkeeping.
const stepUsage = (usage: ThreadTokenUsageSnapshot): StepUsage | undefined => {
  const input = usage.lastInputTokens ?? usage.inputTokens;
  const output = usage.lastOutputTokens ?? usage.outputTokens;
  if (input === undefined || output === undefined) return undefined;
  return {
    input,
    output,
    cached: usage.lastCachedInputTokens ?? usage.cachedInputTokens,
    reasoning: usage.lastReasoningOutputTokens ?? usage.reasoningOutputTokens,
  };
};

const recordUsage = (span: Tracer.Span, usage: StepUsage) => {
  span.attribute("gen_ai.usage.input_tokens", usage.input);
  span.attribute("gen_ai.usage.output_tokens", usage.output);
  span.attribute("gen_ai.usage.total_tokens", usage.input + usage.output);
  if (usage.cached !== undefined) {
    span.attribute("gen_ai.usage.input_tokens.cached", usage.cached);
  }
  if (usage.reasoning !== undefined) {
    span.attribute("gen_ai.usage.output_tokens.reasoning", usage.reasoning);
  }
};

const recordTaskUsage = (span: Tracer.Span, usage: RuntimeTaskUsage) => {
  span.attribute("gen_ai.usage.total_tokens", usage.totalTokens);
  if (usage.inputTokens !== undefined) {
    span.attribute("gen_ai.usage.input_tokens", usage.inputTokens);
  }
  if (usage.outputTokens !== undefined) {
    span.attribute("gen_ai.usage.output_tokens", usage.outputTokens);
  }
};

const applyTaskUpdate = (
  agent: Tracer.Span,
  payload: {
    readonly model?: string | undefined;
    readonly typedUsage?: RuntimeTaskUsage | undefined;
  },
) => {
  if (payload.model) agent.attribute("gen_ai.request.model", payload.model);
  if (payload.typedUsage) recordTaskUsage(agent, payload.typedUsage);
};

interface ToolCallPart {
  readonly type: "tool_call";
  readonly id: RuntimeItemId;
  readonly name: string;
  arguments: string;
}

/**
 * One model response: the `gen_ai.chat` span Sentry renders as a transcript entry.
 * Opens when the model is called (turn start, or the last tool result of the
 * previous response) and closes on the usage snapshot the adapter emits when
 * the response ends. Streamed content opens one lazily as a fallback.
 */
interface ModelStep {
  readonly span: Tracer.Span;
  readonly toolCalls: Map<RuntimeItemId, ToolCallPart>;
  text: string;
}

interface StepInput {
  readonly role: "user" | "tool";
  readonly content: string;
}

interface TrackedTurn {
  readonly span: Tracer.Span;
  readonly threadId: ThreadId;
  readonly model: string | undefined;
  readonly tools: Map<RuntimeItemId, Tracer.Span>;
  readonly usage: StepUsage;
  /** Input for the next model step: the prompt first, then tool results. */
  stepInput: Array<StepInput>;
  step: ModelStep | undefined;
  responseText: string;
}

const make = Effect.gen(function* () {
  const providerService = yield* ProviderService;
  const orchestrationEngine = yield* OrchestrationEngineService;

  const turns = new Map<TurnId, TrackedTurn>();
  const getTurn = (turnId: TurnId | undefined) =>
    turnId !== undefined ? turns.get(turnId) : undefined;

  // Input for a thread's next turn. The decider emits `thread.message-sent`
  // (role user, turnId: null) right before it starts the turn, and a background
  // subagent delivers its report between turns; neither reaches the runtime
  // stream inside the turn that consumes it.
  const pendingInputs = new Map<ThreadId, Array<StepInput>>();
  const pushPendingInput = (threadId: ThreadId, input: StepInput) => {
    const inputs = pendingInputs.get(threadId) ?? [];
    inputs.push(input);
    pendingInputs.set(threadId, inputs);
  };

  /** Trace of the turn-start request, so `invoke_agent` continues it instead of starting a new trace. */
  const pendingTraces = new Map<ThreadId, OrchestrationTraceContext>();

  // Subagent spans keyed by task id. A background agent outlives the turn that
  // launched it and reports progress with no turn id.
  const agents = new Map<RuntimeTaskId, { span: Tracer.Span; threadId: ThreadId }>();

  const endSpan = Effect.fn(function* (span: Tracer.Span, exit: Exit.Exit<unknown, unknown>) {
    const now = yield* Clock.currentTimeNanos;
    span.end(now, exit);
  });

  const openStep = Effect.fn(function* (turn: TrackedTurn, provider: string) {
    if (turn.step !== undefined) return turn.step;
    const span = yield* Effect.makeSpan(`chat ${turn.model ?? provider}`, {
      kind: "internal",
      parent: turn.span,
      attributes: {
        "sentry.op": "gen_ai.chat",
        "gen_ai.operation.name": "chat",
        "gen_ai.system": GEN_AI_SYSTEM[provider] ?? provider,
        "gen_ai.conversation.id": turn.threadId,
        ...(turn.model ? { "gen_ai.request.model": turn.model } : {}),
        ...(turn.stepInput.length > 0
          ? { "gen_ai.input.messages": encodeJson(turn.stepInput) }
          : {}),
      },
    });
    turn.stepInput = [];
    turn.step = { span, toolCalls: new Map(), text: "" };
    return turn.step;
  });

  const closeStep = Effect.fn(function* (
    turn: TrackedTurn,
    exit: Exit.Exit<unknown, unknown>,
    usage?: StepUsage,
  ) {
    const step = turn.step;
    if (step === undefined) return;
    turn.step = undefined;
    const finishReason = step.toolCalls.size > 0 ? "tool_call" : "stop";
    step.span.attribute(
      "gen_ai.output.messages",
      encodeJson([
        {
          role: "assistant",
          parts: [
            ...(step.text !== "" ? [{ type: "text", content: clip(step.text) }] : []),
            ...step.toolCalls.values(),
          ],
          finish_reason: finishReason,
        },
      ]),
    );
    step.span.attribute("gen_ai.response.finish_reasons", encodeFinishReasons([finishReason]));
    if (usage !== undefined) {
      recordUsage(step.span, usage);
      turn.usage.input += usage.input;
      turn.usage.output += usage.output;
      if (usage.cached !== undefined) turn.usage.cached = (turn.usage.cached ?? 0) + usage.cached;
      if (usage.reasoning !== undefined) {
        turn.usage.reasoning = (turn.usage.reasoning ?? 0) + usage.reasoning;
      }
      recordUsage(turn.span, turn.usage);
    }
    yield* endSpan(step.span, exit);
  });

  const endTurn = Effect.fn(function* (turn: TrackedTurn, exit: Exit.Exit<unknown, unknown>) {
    yield* closeStep(turn, exit);
    for (const tool of turn.tools.values()) {
      yield* endSpan(tool, exit);
    }
    if (turn.responseText !== "") {
      turn.span.attribute(
        "gen_ai.output.messages",
        encodeMessages("assistant", clip(turn.responseText)),
      );
    }
    yield* endSpan(turn.span, exit);
  });

  const handle = Effect.fn(function* (event: ProviderRuntimeEvent) {
    switch (event.type) {
      case "turn.started": {
        if (event.turnId === undefined) return;
        const inputs = pendingInputs.get(event.threadId) ?? [];
        pendingInputs.delete(event.threadId);
        const trace = pendingTraces.get(event.threadId);
        pendingTraces.delete(event.threadId);
        const span = yield* Effect.makeSpan(`invoke_agent ${event.provider}`, {
          kind: "internal",
          ...(trace ? { parent: Tracer.externalSpan(trace) } : { root: true }),
          attributes: {
            "sentry.op": "gen_ai.invoke_agent",
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": event.provider,
            "gen_ai.system": GEN_AI_SYSTEM[event.provider] ?? event.provider,
            "gen_ai.conversation.id": event.threadId,
            ...(event.payload.model ? { "gen_ai.request.model": event.payload.model } : {}),
            ...(inputs.length > 0 ? { "gen_ai.input.messages": encodeJson(inputs) } : {}),
            "t3.thread.id": event.threadId,
            "t3.turn.id": event.turnId,
          },
        });
        const turn: TrackedTurn = {
          span,
          threadId: event.threadId,
          model: event.payload.model,
          tools: new Map(),
          usage: { input: 0, output: 0, cached: undefined, reasoning: undefined },
          stepInput: inputs,
          step: undefined,
          responseText: "",
        };
        turns.set(event.turnId, turn);
        yield* openStep(turn, event.provider);
        return;
      }
      case "item.started": {
        const { turnId, itemId } = event;
        const turn = getTurn(turnId);
        if (
          turn === undefined ||
          itemId === undefined ||
          !isToolLifecycleItemType(event.payload.itemType)
        ) {
          return;
        }
        const name = event.payload.title ?? event.payload.itemType;
        const input = toolInput(event.payload.data) ?? event.payload.detail;
        const step = yield* openStep(turn, event.provider);
        step.toolCalls.set(itemId, {
          type: "tool_call",
          id: itemId,
          name,
          arguments: input !== undefined ? encodeJson(input) : "",
        });
        const owner = event.payload.agentId
          ? agents.get(event.payload.agentId as RuntimeTaskId)?.span
          : undefined;
        const tool = yield* Effect.makeSpan(`execute_tool ${name}`, {
          kind: "internal",
          parent: owner ?? turn.span,
          attributes: {
            "sentry.op": "gen_ai.execute_tool",
            "gen_ai.operation.name": "execute_tool",
            "gen_ai.conversation.id": turn.threadId,
            "gen_ai.tool.name": name,
            "gen_ai.tool.type": event.payload.itemType,
            "gen_ai.tool.call.id": itemId,
            ...(input !== undefined ? { "gen_ai.tool.input": encodeJson(input) } : {}),
          },
        });
        turn.tools.set(itemId, tool);
        return;
      }
      case "item.updated": {
        // Claude streams tool input after item.started; the input is only complete here.
        const turn = getTurn(event.turnId);
        const tool = event.itemId !== undefined ? turn?.tools.get(event.itemId) : undefined;
        const input = toolInput(event.payload.data);
        if (turn === undefined || tool === undefined || input === undefined) return;
        const encoded = encodeJson(input);
        tool.attribute("gen_ai.tool.input", encoded);
        const part =
          event.itemId !== undefined ? turn.step?.toolCalls.get(event.itemId) : undefined;
        if (part !== undefined) part.arguments = encoded;
        return;
      }
      case "item.completed": {
        const { turnId, itemId } = event;
        const turn = getTurn(turnId);
        const tool =
          turn !== undefined && itemId !== undefined ? turn.tools.get(itemId) : undefined;
        if (turn === undefined || tool === undefined || itemId === undefined) return;
        const output = toolOutput(event.payload.data) ?? event.payload.detail;
        if (output !== undefined) {
          const encoded = encodeJson(output);
          tool.attribute("gen_ai.tool.output", encoded);
          turn.stepInput.push({ role: "tool", content: encoded });
        }
        yield* endSpan(
          tool,
          event.payload.status === "failed"
            ? Exit.fail(event.payload.detail ?? "tool failed")
            : Exit.succeed(undefined),
        );
        turn.tools.delete(itemId);
        if (turn.tools.size === 0) {
          yield* openStep(turn, event.provider);
        }
        return;
      }
      case "task.started": {
        const turn = getTurn(event.turnId);
        const { payload } = event;
        if (turn === undefined || classifyTaskAgentKind(payload) !== "agent") return;
        const name = payload.role ?? payload.title ?? "agent";
        // Nest under the tool call that launched the subagent when the adapter links them.
        const launcher = payload.toolUseId
          ? turn.tools.get(payload.toolUseId as RuntimeItemId)
          : undefined;
        const agent = yield* Effect.makeSpan(`invoke_agent ${name}`, {
          kind: "internal",
          parent: launcher ?? turn.span,
          attributes: {
            "sentry.op": "gen_ai.invoke_agent",
            "gen_ai.operation.name": "invoke_agent",
            "gen_ai.agent.name": name,
            "gen_ai.system": GEN_AI_SYSTEM[event.provider] ?? event.provider,
            "gen_ai.conversation.id": turn.threadId,
            ...(payload.model ? { "gen_ai.request.model": payload.model } : {}),
            ...(payload.description
              ? { "gen_ai.input.messages": encodeMessages("user", clip(payload.description)) }
              : {}),
            "t3.task.id": payload.taskId,
          },
        });
        agents.set(payload.taskId, { span: agent, threadId: turn.threadId });
        return;
      }
      case "task.progress": {
        const agent = agents.get(event.payload.taskId)?.span;
        if (agent === undefined) return;
        // Progress rows carry the model the subagent actually ran on, which can
        // differ from the launch-time seed.
        applyTaskUpdate(agent, event.payload);
        return;
      }
      case "task.completed": {
        const agent = agents.get(event.payload.taskId);
        if (agent === undefined) return;
        const { payload } = event;
        applyTaskUpdate(agent.span, payload);
        if (payload.summary) {
          const summary = clip(payload.summary);
          agent.span.attribute("gen_ai.output.messages", encodeMessages("assistant", summary));
          // Delivered between turns, the report becomes the input of the
          // synthetic turn the adapter starts to hand it to the model.
          if (event.turnId === undefined) {
            pushPendingInput(agent.threadId, { role: "tool", content: summary });
          }
        }
        yield* endSpan(
          agent.span,
          payload.status === "completed" ? Exit.succeed(undefined) : Exit.fail(payload.status),
        );
        agents.delete(payload.taskId);
        return;
      }
      case "content.delta": {
        const turn = getTurn(event.turnId);
        if (turn === undefined) return;
        const { streamKind, delta } = event.payload;
        if (streamKind !== "assistant_text" && streamKind !== "reasoning_text") return;
        const step = yield* openStep(turn, event.provider);
        if (streamKind !== "assistant_text") return;
        if (step.text.length < MAX_CONTENT_CHARS) step.text += delta;
        if (turn.responseText.length < MAX_CONTENT_CHARS) turn.responseText += delta;
        return;
      }
      case "thread.token-usage.updated": {
        const turn = getTurn(event.turnId);
        const usage = stepUsage(event.payload.usage);
        if (turn === undefined || usage === undefined) return;
        yield* closeStep(turn, Exit.succeed(undefined), usage);
        return;
      }
      case "turn.completed": {
        const { turnId } = event;
        const turn = getTurn(turnId);
        if (turn === undefined || turnId === undefined) return;
        turn.span.attribute(
          "gen_ai.response.finish_reasons",
          encodeFinishReasons([event.payload.stopReason ?? event.payload.state]),
        );
        yield* endTurn(
          turn,
          event.payload.state === "completed"
            ? Exit.succeed(undefined)
            : event.payload.state === "failed"
              ? Exit.fail(event.payload.errorMessage ?? event.payload.state)
              : Exit.interrupt(),
        );
        turns.delete(turnId);
        return;
      }
      case "turn.aborted": {
        const { turnId } = event;
        const turn = getTurn(turnId);
        if (turn === undefined || turnId === undefined) return;
        yield* endTurn(turn, Exit.fail(event.payload.reason));
        turns.delete(turnId);
        return;
      }
      case "session.exited": {
        for (const [turnId, turn] of turns) {
          if (turn.threadId !== event.threadId) continue;
          yield* endTurn(turn, Exit.fail("session exited"));
          turns.delete(turnId);
        }
        for (const [taskId, agent] of agents) {
          if (agent.threadId !== event.threadId) continue;
          yield* endSpan(agent.span, Exit.fail("session exited"));
          agents.delete(taskId);
        }
        return;
      }
      default:
        return;
    }
  });

  const handleDomainEvent = (event: OrchestrationEvent) =>
    Effect.sync(() => {
      if (event.type === "thread.message-sent" && event.payload.role === "user") {
        pushPendingInput(event.payload.threadId, {
          role: "user",
          content: clip(event.payload.text),
        });
      }
      if (event.type === "thread.turn-start-requested" && event.metadata.trace) {
        pendingTraces.set(event.payload.threadId, event.metadata.trace);
      }
    });

  const runSafely = <A>(effect: Effect.Effect<A>, eventType: string) =>
    effect.pipe(
      Effect.catchCause((cause) => {
        if (Cause.hasInterruptsOnly(cause)) {
          return Effect.failCause(cause);
        }
        return Effect.logWarning("provider turn tracing failed", {
          eventType,
          cause: Cause.pretty(cause),
        });
      }),
    );

  const start: ProviderTurnTracingShape["start"] = Effect.fn("start")(function* () {
    yield* forkParked(
      Stream.runForEach(providerService.streamEvents, (event) =>
        runSafely(handle(event), event.type),
      ),
    );
    yield* forkParked(
      Stream.runForEach(orchestrationEngine.streamDomainEvents, (event) =>
        runSafely(handleDomainEvent(event), event.type),
      ),
    );
  });

  return { start } satisfies ProviderTurnTracingShape;
});

export const ProviderTurnTracingLive = Layer.effect(ProviderTurnTracing, make);
