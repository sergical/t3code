// @effect-diagnostics nodeBuiltinImport:off - spike harness reads env gating from process.env.
import {
  CommandId,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient } from "effect/unstable/http";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer";

import { sentryOtlpTraces } from "../src/observability/sentryOtlp.ts";
import { makeOrchestrationIntegrationHarness } from "./OrchestrationEngineHarness.integration.ts";

const PROJECT_ID = ProjectId.make("project-otel-spike");
const THREAD_ID = ThreadId.make("thread-otel-spike");
const NOW = "2026-08-31T00:00:00.000Z";
const MODEL_SELECTION = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.3-codex",
};
const REAL_MODEL_SELECTION = {
  instanceId: ProviderInstanceId.make("codex"),
  model: process.env.T3CODE_TEST_CODEX_MODEL ?? "gpt-5.3-codex",
};

/**
 * Spike: drives one scripted agent turn through the orchestration engine with
 * the live ProviderTurnTracing reactor, exporting spans to Sentry's OTLP
 * ingest endpoint. Run with SENTRY_DSN set, then inspect the gen_ai spans in
 * Sentry to compare against the Sentry-SDK trace path.
 */
it.live.skipIf(!process.env.SENTRY_DSN)(
  "exports a real agent turn's gen_ai spans to Sentry over OTLP",
  () => {
    const sentry = sentryOtlpTraces(process.env.SENTRY_DSN ?? "");
    assert.ok(sentry, "SENTRY_DSN must parse");
    const tracerLayer = OtlpTracer.layer({
      url: sentry.url,
      headers: sentry.headers,
      exportInterval: "200 millis",
      resource: {
        serviceName: "t3-server-otel-spike",
        attributes: {
          "service.runtime": "t3-server",
          "service.mode": "web",
          "deployment.environment.name": "development",
        },
      },
    }).pipe(
      Layer.provideMerge(OtlpSerialization.layerJson),
      Layer.provide(FetchHttpClient.layer),
    ) as Layer.Layer<never>;

    return Effect.acquireUseRelease(
      makeOrchestrationIntegrationHarness({
        provider: ProviderDriverKind.make("codex"),
        liveTurnTracing: true,
        extraLayer: tracerLayer,
        configOverrides: { traceGenAiContent: true },
      }),
      (harness) =>
        Effect.gen(function* () {
          const createdAt = NOW;
          yield* harness.engine.dispatch({
            type: "project.create",
            commandId: CommandId.make("cmd-project-otel-spike"),
            projectId: PROJECT_ID,
            title: "OTel Spike",
            workspaceRoot: harness.workspaceDir,
            defaultModelSelection: MODEL_SELECTION,
            createdAt,
          });
          yield* harness.engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("cmd-thread-otel-spike"),
            threadId: THREAD_ID,
            projectId: PROJECT_ID,
            title: "OTel Spike Thread",
            modelSelection: MODEL_SELECTION,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: harness.workspaceDir,
            createdAt,
          });
          yield* harness.adapterHarness!.queueTurnResponseForNextSession({
            events: [
              {
                type: "turn.started",
                eventId: EventId.make("evt-otel-1"),
                provider: ProviderDriverKind.make("codex"),
                createdAt: NOW,
                threadId: THREAD_ID,
                turnId: "otel-spike-turn",
              },
              {
                type: "tool.started",
                itemId: "tool-otel-1",
                eventId: EventId.make("evt-otel-2"),
                provider: ProviderDriverKind.make("codex"),
                createdAt: NOW,
                threadId: THREAD_ID,
                turnId: "otel-spike-turn",
                toolKind: "command",
                title: "Write file",
                detail: "hello.txt",
              },
              {
                type: "tool.completed",
                itemId: "tool-otel-1",
                eventId: EventId.make("evt-otel-3"),
                provider: ProviderDriverKind.make("codex"),
                createdAt: NOW,
                threadId: THREAD_ID,
                turnId: "otel-spike-turn",
                toolKind: "command",
                title: "Write file",
                detail: "hello.txt",
              },
              {
                type: "message.delta",
                eventId: EventId.make("evt-otel-4"),
                provider: ProviderDriverKind.make("codex"),
                createdAt: NOW,
                threadId: THREAD_ID,
                turnId: "otel-spike-turn",
                delta: "DONE\n",
              },
              {
                type: "turn.completed",
                eventId: EventId.make("evt-otel-5"),
                provider: ProviderDriverKind.make("codex"),
                createdAt: NOW,
                threadId: THREAD_ID,
                turnId: "otel-spike-turn",
                status: "completed",
              },
            ],
          });
          yield* harness.engine.dispatch({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-turn-otel-spike-1"),
            threadId: THREAD_ID,
            message: {
              messageId: MessageId.make("msg-otel-spike-1"),
              role: "user",
              text: "Create a file named hello.txt containing the word hello, then reply DONE.",
              attachments: [],
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            createdAt: NOW,
          });
          const thread = yield* harness.waitForThread(
            THREAD_ID,
            (entry) =>
              entry.session?.status === "ready" &&
              entry.messages.some(
                (message) => message.role === "assistant" && message.streaming === false,
              ),
            180_000,
          );
          assert.ok(thread.session);
        }),
      (harness) => harness.dispose,
    ).pipe(Effect.provide(NodeServices.layer));
  },
  240_000,
);

const REAL_PROJECT_ID = ProjectId.make("project-otel-real");
const REAL_THREAD_ID = ThreadId.make("thread-otel-real");

/**
 * Spike: same export path, but with the real Codex CLI so the turn carries a
 * real model and real token usage. Needs SENTRY_DSN plus CODEX_BINARY_PATH
 * pointing at an authenticated codex binary.
 */
it.live.skipIf(!process.env.SENTRY_DSN || !process.env.CODEX_BINARY_PATH)(
  "exports a real codex turn with model and usage to Sentry over OTLP",
  () => {
    const sentry = sentryOtlpTraces(process.env.SENTRY_DSN ?? "");
    assert.ok(sentry, "SENTRY_DSN must parse");
    const tracerLayer = OtlpTracer.layer({
      url: sentry.url,
      headers: sentry.headers,
      exportInterval: "200 millis",
      resource: {
        serviceName: "t3-server-otel-spike",
        attributes: {
          "service.runtime": "t3-server",
          "service.mode": "web",
          "deployment.environment.name": "development",
        },
      },
    }).pipe(
      Layer.provideMerge(OtlpSerialization.layerJson),
      Layer.provide(FetchHttpClient.layer),
    ) as Layer.Layer<never>;

    return Effect.acquireUseRelease(
      makeOrchestrationIntegrationHarness({
        provider: ProviderDriverKind.make("codex"),
        realCodex: true,
        liveTurnTracing: true,
        extraLayer: tracerLayer,
        configOverrides: { traceGenAiContent: true },
      }),
      (harness) =>
        Effect.gen(function* () {
          yield* harness.engine.dispatch({
            type: "project.create",
            commandId: CommandId.make("cmd-project-otel-real"),
            projectId: REAL_PROJECT_ID,
            title: "OTel Spike Real",
            workspaceRoot: harness.workspaceDir,
            defaultModelSelection: REAL_MODEL_SELECTION,
            createdAt: NOW,
          });
          yield* harness.engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("cmd-thread-otel-real"),
            threadId: REAL_THREAD_ID,
            projectId: REAL_PROJECT_ID,
            title: "OTel Spike Real Thread",
            modelSelection: REAL_MODEL_SELECTION,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            branch: null,
            worktreePath: harness.workspaceDir,
            createdAt: NOW,
          });
          yield* harness.engine.dispatch({
            type: "thread.turn.start",
            commandId: CommandId.make("cmd-turn-otel-real-1"),
            threadId: REAL_THREAD_ID,
            modelSelection: REAL_MODEL_SELECTION,
            message: {
              messageId: MessageId.make("msg-otel-real-1"),
              role: "user",
              text: "Create a file named hello.txt containing the word hello, then reply DONE.",
              attachments: [],
            },
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            runtimeMode: "full-access",
            createdAt: NOW,
          });
          const thread = yield* harness.waitForThread(
            REAL_THREAD_ID,
            (entry) =>
              entry.session?.status === "ready" &&
              entry.messages.some(
                (message) => message.role === "assistant" && message.streaming === false,
              ),
            180_000,
          );
          assert.ok(thread.session);
        }),
      (harness) => harness.dispose,
    ).pipe(Effect.provide(NodeServices.layer));
  },
  240_000,
);
