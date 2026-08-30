import {
  EventId,
  MessageId,
  ProviderDriverKind,
  RuntimeItemId,
  RuntimeTaskId,
  ThreadId,
  TurnId,
  type ItemLifecyclePayload,
  type OrchestrationEvent,
  type OrchestrationTraceContext,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import { assert, describe, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";

import { ProviderService } from "../../provider/Services/ProviderService.ts";
import { OrchestrationEngineService } from "../Services/OrchestrationEngine.ts";
import { ProviderTurnTracing } from "../Services/ProviderTurnTracing.ts";
import { ProviderTurnTracingLive } from "./ProviderTurnTracing.ts";

const threadId = ThreadId.make("thread-1");
const turnId = TurnId.make("turn-1");
const itemId = RuntimeItemId.make("item-1");
const provider = ProviderDriverKind.make("claude");

let eventCounter = 0;
const nextEventId = () => EventId.make(`event-${++eventCounter}`);
const now = () => "2026-01-01T00:00:00.000Z";

const turnStarted = (): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  type: "turn.started",
  payload: {},
});

const itemStarted = (payload: Partial<ItemLifecyclePayload> = {}): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  itemId,
  type: "item.started",
  payload: { itemType: "command_execution", ...payload },
});

const itemCompleted = (payload: Partial<ItemLifecyclePayload> = {}): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  itemId,
  type: "item.completed",
  payload: { itemType: "command_execution", status: "completed", ...payload },
});

const itemUpdated = (payload: Partial<ItemLifecyclePayload> = {}): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  itemId,
  type: "item.updated",
  payload: { itemType: "command_execution", status: "inProgress", ...payload },
});

const taskId = RuntimeTaskId.make("task-1");

const taskStarted = (): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  type: "task.started",
  payload: {
    taskId,
    taskType: "local_agent",
    role: "Explore",
    model: "claude-fable-5",
    description: "Map the repo",
    toolUseId: itemId,
  },
});

// Background agents report after their launching turn ended, with no turn id.
const taskProgress = (background = false): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  ...(background ? {} : { turnId }),
  type: "task.progress",
  payload: {
    taskId,
    description: "Map the repo",
    model: "claude-haiku-4-5",
    typedUsage: { totalTokens: 100, toolUses: 3 },
  },
});

const taskCompleted = (background = false): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  ...(background ? {} : { turnId }),
  type: "task.completed",
  payload: {
    taskId,
    status: "completed",
    summary: "Found it.",
    typedUsage: { totalTokens: 120, toolUses: 4 },
  },
});

const contentDelta = (delta: string): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  type: "content.delta",
  payload: { streamKind: "assistant_text", delta },
});

let messageCounter = 0;
const messageSent = (text: string): OrchestrationEvent =>
  ({
    sequence: ++messageCounter,
    eventId: nextEventId(),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now(),
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: {},
    type: "thread.message-sent",
    payload: {
      threadId,
      messageId: MessageId.make(`message-${messageCounter}`),
      role: "user",
      text,
      turnId: null,
      streaming: false,
      createdAt: now(),
      updatedAt: now(),
    },
  }) as OrchestrationEvent;

const turnStartRequested = (trace: OrchestrationTraceContext): OrchestrationEvent =>
  ({
    sequence: ++messageCounter,
    eventId: nextEventId(),
    aggregateKind: "thread",
    aggregateId: threadId,
    occurredAt: now(),
    commandId: null,
    causationEventId: null,
    correlationId: null,
    metadata: { trace },
    type: "thread.turn-start-requested",
    payload: {
      threadId,
      messageId: MessageId.make(`message-${messageCounter}`),
      runtimeMode: "full-access",
      interactionMode: "default",
      createdAt: now(),
    },
  }) as OrchestrationEvent;

const tokenUsageUpdated = (inputTokens = 10, outputTokens = 5): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  type: "thread.token-usage.updated",
  payload: { usage: { usedTokens: inputTokens + outputTokens, inputTokens, outputTokens } },
});

const turnCompleted = (
  state: "completed" | "failed",
  errorMessage?: string,
): ProviderRuntimeEvent => ({
  eventId: nextEventId(),
  provider,
  threadId,
  createdAt: now(),
  turnId,
  type: "turn.completed",
  payload: { state, ...(errorMessage !== undefined ? { errorMessage } : {}) },
});

/**
 * Runs the reactor over `events` (and optional `domainEvents`), waiting for both
 * finite streams to drain, then hands back every span the tracer saw. The domain
 * stream is guaranteed to be fully consumed before the provider stream starts, so
 * a `thread.message-sent` reliably lands before the `turn.started` that reads it.
 */
const runScenario = (
  events: ReadonlyArray<ProviderRuntimeEvent>,
  domainEvents: ReadonlyArray<OrchestrationEvent> = [],
) =>
  Effect.gen(function* () {
    const spans: Array<Tracer.NativeSpan> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        spans.push(span);
        return span;
      },
    });

    const domainDrained = yield* Deferred.make<void>();
    const orchestrationEngineLayer = Layer.succeed(OrchestrationEngineService, {
      streamDomainEvents: Stream.fromIterable(domainEvents).pipe(
        Stream.ensuring(Deferred.succeed(domainDrained, undefined)),
      ),
    } as OrchestrationEngineService["Service"]);

    const drained = yield* Deferred.make<void>();
    const providerServiceLayer = Layer.succeed(ProviderService, {
      // Waits for the domain stream to fully drain first, so a stashed prompt is
      // always in place before this stream's `turn.started` looks for it.
      streamEvents: Stream.fromEffect(Deferred.await(domainDrained)).pipe(
        Stream.flatMap(() => Stream.fromIterable(events)),
        Stream.ensuring(Deferred.succeed(drained, undefined)),
      ),
    } as ProviderService["Service"]);

    yield* Effect.scoped(
      Effect.gen(function* () {
        const providerTurnTracing = yield* ProviderTurnTracing;
        yield* providerTurnTracing.start();
        yield* Deferred.await(drained);
      }),
    ).pipe(
      Effect.provide(
        ProviderTurnTracingLive.pipe(
          Layer.provide(Layer.merge(providerServiceLayer, orchestrationEngineLayer)),
        ),
      ),
      Effect.withTracer(tracer),
    );

    return spans;
  });

describe("ProviderTurnTracing", () => {
  it.effect(
    "produces a gen_ai.invoke_agent span with a nested execute_tool span and usage totals",
    () =>
      Effect.gen(function* () {
        const spans = yield* runScenario(
          [
            turnStarted(),
            itemStarted({ data: { toolName: "Bash", input: { command: "ls" } } }),
            contentDelta("Here are"),
            contentDelta(" the files."),
            itemCompleted({ data: { result: "a.ts\nb.ts" } }),
            tokenUsageUpdated(),
            turnCompleted("completed"),
          ],
          [messageSent("list files")],
        );

        const turnSpan = spans.find((span) => span.name === `invoke_agent ${provider}`);
        const toolSpan = spans.find((span) => span.name.startsWith("execute_tool"));
        const chatSpan = spans.find((span) => span.name.startsWith("chat "));
        assert.notEqual(turnSpan, undefined);
        assert.notEqual(toolSpan, undefined);
        assert.notEqual(chatSpan, undefined);
        if (turnSpan === undefined || toolSpan === undefined || chatSpan === undefined) {
          return;
        }

        assert.equal(turnSpan.attributes.get("gen_ai.conversation.id"), threadId);
        assert.equal(
          turnSpan.attributes.get("gen_ai.input.messages"),
          '[{"role":"user","content":"list files"}]',
        );
        assert.equal(
          turnSpan.attributes.get("gen_ai.output.messages"),
          '[{"role":"assistant","content":"Here are the files."}]',
        );
        assert.equal(turnSpan.status._tag, "Ended");
        if (turnSpan.status._tag === "Ended") {
          assert.equal(turnSpan.status.exit._tag, "Success");
        }

        assert.equal(Option.getOrUndefined(chatSpan.parent), turnSpan);
        assert.equal(chatSpan.attributes.get("sentry.op"), "gen_ai.chat");
        assert.equal(chatSpan.attributes.get("gen_ai.conversation.id"), threadId);
        assert.equal(
          chatSpan.attributes.get("gen_ai.input.messages"),
          '[{"role":"user","content":"list files"}]',
        );
        assert.equal(
          chatSpan.attributes.get("gen_ai.output.messages"),
          '[{"role":"assistant","parts":[{"type":"text","content":"Here are the files."},{"type":"tool_call","id":"item-1","name":"command_execution","arguments":"{\\"command\\":\\"ls\\"}"}],"finish_reason":"tool_call"}]',
        );
        assert.equal(chatSpan.attributes.get("gen_ai.response.finish_reasons"), '["tool_call"]');
        assert.equal(chatSpan.attributes.get("gen_ai.usage.input_tokens"), 10);
        assert.equal(chatSpan.attributes.get("gen_ai.usage.output_tokens"), 5);
        assert.equal(chatSpan.status._tag, "Ended");

        assert.equal(Option.getOrUndefined(toolSpan.parent), turnSpan);
        assert.equal(toolSpan.attributes.get("gen_ai.conversation.id"), threadId);
        assert.equal(toolSpan.attributes.get("gen_ai.tool.call.id"), itemId);
        assert.equal(toolSpan.attributes.get("gen_ai.tool.input"), '{"command":"ls"}');
        assert.equal(toolSpan.attributes.get("gen_ai.tool.output"), '"a.ts\\nb.ts"');
        assert.equal(toolSpan.status._tag, "Ended");
        if (toolSpan.status._tag === "Ended") {
          assert.equal(toolSpan.status.exit._tag, "Success");
        }
      }),
  );

  it.effect("opens one chat span per model response and sums usage on the turn span", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([
        turnStarted(),
        itemStarted({ data: { toolName: "Bash", input: { command: "ls" } } }),
        itemCompleted({ data: { result: "a.ts" } }),
        tokenUsageUpdated(10, 5),
        contentDelta("Done."),
        tokenUsageUpdated(20, 7),
        turnCompleted("completed"),
      ]);

      const turnSpan = spans.find((span) => span.name === `invoke_agent ${provider}`);
      const chatSpans = spans.filter((span) => span.name.startsWith("chat "));
      assert.equal(chatSpans.length, 2);
      const [toolStep, textStep] = chatSpans;
      if (turnSpan === undefined || toolStep === undefined || textStep === undefined) {
        return;
      }

      assert.equal(toolStep.attributes.get("gen_ai.response.finish_reasons"), '["tool_call"]');
      assert.equal(toolStep.attributes.get("gen_ai.usage.input_tokens"), 10);
      assert.equal(
        textStep.attributes.get("gen_ai.input.messages"),
        '[{"role":"tool","content":"\\"a.ts\\""}]',
      );
      assert.equal(
        textStep.attributes.get("gen_ai.output.messages"),
        '[{"role":"assistant","parts":[{"type":"text","content":"Done."}],"finish_reason":"stop"}]',
      );
      assert.equal(textStep.attributes.get("gen_ai.usage.input_tokens"), 20);
      assert.equal(textStep.attributes.get("gen_ai.usage.output_tokens"), 7);
      assert.equal(turnSpan.attributes.get("gen_ai.usage.input_tokens"), 30);
      assert.equal(turnSpan.attributes.get("gen_ai.usage.output_tokens"), 12);
      assert.equal(turnSpan.attributes.get("gen_ai.usage.total_tokens"), 42);
    }),
  );

  it.effect("ignores usage snapshots that arrive outside a model response", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([
        turnStarted(),
        contentDelta("Hi"),
        tokenUsageUpdated(10, 5),
        // Claude's turn-end `result` usage repeats the totals after the last response.
        tokenUsageUpdated(999, 999),
        turnCompleted("completed"),
      ]);

      const turnSpan = spans.find((span) => span.name === `invoke_agent ${provider}`);
      assert.equal(spans.filter((span) => span.name.startsWith("chat ")).length, 1);
      assert.equal(turnSpan?.attributes.get("gen_ai.usage.total_tokens"), 15);
    }),
  );

  it.effect("keeps a background subagent open past its turn and feeds its report to the next", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([
        turnStarted(),
        itemStarted({ itemType: "collab_agent_tool_call", title: "Agent" }),
        taskStarted(),
        itemCompleted({ itemType: "collab_agent_tool_call" }),
        turnCompleted("completed"),
        taskProgress(true),
        taskCompleted(true),
        turnStarted(),
      ]);

      const agentSpan = spans.find((span) => span.name === "invoke_agent Explore");
      assert.equal(agentSpan?.attributes.get("gen_ai.request.model"), "claude-haiku-4-5");
      assert.equal(agentSpan?.status._tag, "Ended");
      if (agentSpan?.status._tag === "Ended") {
        assert.equal(agentSpan.status.exit._tag, "Success");
      }
      const nextTurn = spans.filter((span) => span.name === `invoke_agent ${provider}`)[1];
      assert.equal(
        nextTurn?.attributes.get("gen_ai.input.messages"),
        '[{"role":"tool","content":"Found it."}]',
      );
    }),
  );

  it.effect("ends an open model response with the turn's exit", () =>
    Effect.gen(function* () {
      const failed = yield* runScenario([
        turnStarted(),
        contentDelta("partial"),
        turnCompleted("failed", "boom"),
      ]);
      const failedStep = failed.find((span) => span.name.startsWith("chat "));
      assert.equal(failedStep?.status._tag, "Ended");
      if (failedStep?.status._tag === "Ended") {
        assert.equal(failedStep.status.exit._tag, "Failure");
      }
    }),
  );

  it.effect("refreshes the tool input from item.updated when the adapter streams it", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([
        turnStarted(),
        itemStarted({ data: { toolName: "Agent", input: {} } }),
        itemUpdated({ data: { toolName: "Agent", input: { prompt: "map the repo" } } }),
        itemCompleted({
          data: { toolName: "Agent", input: { prompt: "map the repo" }, result: "done" },
        }),
        turnCompleted("completed"),
      ]);

      const toolSpan = spans.find((span) => span.name.startsWith("execute_tool"));
      assert.notEqual(toolSpan, undefined);
      assert.equal(toolSpan?.attributes.get("gen_ai.tool.input"), '{"prompt":"map the repo"}');
      assert.equal(toolSpan?.attributes.get("gen_ai.tool.output"), '"done"');
    }),
  );

  it.effect("nests a subagent invoke_agent span under the tool call that launched it", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([
        turnStarted(),
        itemStarted({ itemType: "collab_agent_tool_call", title: "Agent" }),
        taskStarted(),
        taskProgress(),
        taskCompleted(),
        itemCompleted({ itemType: "collab_agent_tool_call" }),
        turnCompleted("completed"),
      ]);

      const toolSpan = spans.find((span) => span.name === "execute_tool Agent");
      const agentSpan = spans.find((span) => span.name === "invoke_agent Explore");
      assert.notEqual(toolSpan, undefined);
      assert.notEqual(agentSpan, undefined);
      if (toolSpan === undefined || agentSpan === undefined) {
        return;
      }

      assert.equal(Option.getOrUndefined(agentSpan.parent), toolSpan);
      assert.equal(agentSpan.attributes.get("sentry.op"), "gen_ai.invoke_agent");
      assert.equal(agentSpan.attributes.get("gen_ai.agent.name"), "Explore");
      assert.equal(agentSpan.attributes.get("gen_ai.request.model"), "claude-haiku-4-5");
      assert.equal(agentSpan.attributes.get("gen_ai.usage.total_tokens"), 120);
      assert.equal(
        agentSpan.attributes.get("gen_ai.input.messages"),
        '[{"role":"user","content":"Map the repo"}]',
      );
      assert.equal(
        agentSpan.attributes.get("gen_ai.output.messages"),
        '[{"role":"assistant","content":"Found it."}]',
      );
      assert.equal(agentSpan.status._tag, "Ended");
      if (agentSpan.status._tag === "Ended") {
        assert.equal(agentSpan.status.exit._tag, "Success");
      }
    }),
  );

  it.effect("clips an oversized assistant response before recording it on the turn span", () =>
    Effect.gen(function* () {
      const oversized = "x".repeat(20_000);
      const spans = yield* runScenario([
        turnStarted(),
        contentDelta(oversized),
        turnCompleted("completed"),
      ]);

      const turnSpan = spans.find((span) => span.name === `invoke_agent ${provider}`);
      assert.notEqual(turnSpan, undefined);
      if (turnSpan === undefined) {
        return;
      }
      const output = turnSpan.attributes.get("gen_ai.output.messages") as string;
      assert.notEqual(output, undefined);
      assert.isTrue(output.includes("…[truncated]"));
      assert.isTrue(output.length < 16_200);
    }),
  );

  it.effect("ends the turn span with a failure exit when the turn fails", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario([turnStarted(), turnCompleted("failed", "boom")]);

      const turnSpan = spans.find((span) => span.name === `invoke_agent ${provider}`);
      assert.notEqual(turnSpan, undefined);
      if (turnSpan === undefined) {
        return;
      }
      assert.equal(turnSpan.status._tag, "Ended");
      if (turnSpan.status._tag === "Ended") {
        assert.equal(turnSpan.status.exit._tag, "Failure");
      }
    }),
  );

  it.effect("invoke_agent continues the trace of the turn-start request", () =>
    Effect.gen(function* () {
      const spans = yield* runScenario(
        [turnStarted(), turnCompleted("completed")],
        [turnStartRequested({ traceId: "trace-request", spanId: "span-request", sampled: true })],
      );

      const turnSpan = spans.find((span) => span.name.startsWith("invoke_agent"));
      assert.notEqual(turnSpan, undefined);
      if (turnSpan === undefined) {
        return;
      }
      assert.equal(turnSpan.traceId, "trace-request");
      assert.isTrue(Option.isSome(turnSpan.parent));
      if (Option.isSome(turnSpan.parent)) {
        assert.equal(turnSpan.parent.value.spanId, "span-request");
      }
    }),
  );
});
