import { assert, describe, it } from "@effect/vitest";
import { WS_METHODS, WsClientTraceMiddleware } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";
import * as TestClock from "effect/testing/TestClock";
import { Headers } from "effect/unstable/http";

import {
  clientTraceMiddlewareLayer,
  externalSpanFromHeaders,
  observeRpcEffect,
  observeRpcStream,
  observeRpcStreamEffect,
  RpcClientSpan,
} from "./RpcInstrumentation.ts";

const hasMetricSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.some(
    (snapshot) =>
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

const findHistogramSnapshot = (
  snapshots: ReadonlyArray<Metric.Metric.Snapshot>,
  id: string,
  attributes: Readonly<Record<string, string>>,
) =>
  snapshots.find(
    (snapshot): snapshot is Extract<Metric.Metric.Snapshot, { readonly type: "Histogram" }> =>
      snapshot.type === "Histogram" &&
      snapshot.id === id &&
      Object.entries(attributes).every(([key, value]) => snapshot.attributes?.[key] === value),
  );

const collectSpanNames = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<ReadonlyArray<string>, E, R> =>
  Effect.gen(function* () {
    const spanNames: Array<string> = [];
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);

        span.end = (endTime, exit) => {
          end(endTime, exit);
          if (span.sampled) {
            spanNames.push(span.name);
          }
        };

        return span;
      },
    });

    yield* effect.pipe(Effect.withTracer(tracer));

    return spanNames;
  });

describe("RpcInstrumentation", () => {
  it.effect("records success metrics for unary RPC handlers", () =>
    Effect.gen(function* () {
      yield* observeRpcEffect("rpc.instrumentation.success", Effect.succeed("ok"), {
        "rpc.aggregate": "test",
      }).pipe(Effect.withSpan("rpc.instrumentation.success.span"));

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_requests_total", {
          method: "rpc.instrumentation.success",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_request_duration", {
          method: "rpc.instrumentation.success",
        }),
        true,
      );
    }),
  );

  it.effect("records failure outcomes for unary RPC handlers", () =>
    Effect.gen(function* () {
      yield* Effect.exit(
        observeRpcEffect("rpc.instrumentation.failure", Effect.fail("boom"), {
          "rpc.aggregate": "test",
        }).pipe(Effect.withSpan("rpc.instrumentation.failure.span")),
      );

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_requests_total", {
          method: "rpc.instrumentation.failure",
          outcome: "failure",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_request_duration", {
          method: "rpc.instrumentation.failure",
        }),
        true,
      );
    }),
  );

  it.effect("records subscription activation metrics for stream RPC handlers", () =>
    Effect.gen(function* () {
      const events = yield* Stream.runCollect(
        observeRpcStreamEffect(
          "rpc.instrumentation.stream",
          Effect.succeed(Stream.make("a", "b")),
          { "rpc.aggregate": "test" },
        ).pipe(Stream.withSpan("rpc.instrumentation.stream.span")),
      );

      assert.deepStrictEqual(Array.from(events), ["a", "b"]);

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_requests_total", {
          method: "rpc.instrumentation.stream",
          outcome: "success",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_request_duration", {
          method: "rpc.instrumentation.stream",
        }),
        true,
      );
    }),
  );

  it.effect("records failure outcomes for direct stream RPC handlers during consumption", () =>
    Effect.gen(function* () {
      const exit = yield* Stream.runCollect(
        observeRpcStream(
          "rpc.instrumentation.stream.failure",
          Stream.make("a").pipe(Stream.concat(Stream.fail("boom"))),
          { "rpc.aggregate": "test" },
        ).pipe(Stream.withSpan("rpc.instrumentation.stream.failure.span")),
      ).pipe(Effect.exit);

      assert.equal(Exit.isFailure(exit), true);

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_requests_total", {
          method: "rpc.instrumentation.stream.failure",
          outcome: "failure",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_request_duration", {
          method: "rpc.instrumentation.stream.failure",
        }),
        true,
      );
    }),
  );

  it.effect("records direct stream durations from nanosecond clock readings", () =>
    Effect.gen(function* () {
      const duration = Duration.nanos(1_500_000n);
      const events = yield* Effect.gen(function* () {
        const fiber = yield* Stream.runCollect(
          observeRpcStream(
            WS_METHODS.serverGetProcessDiagnostics,
            Stream.fromEffect(Effect.sleep(duration).pipe(Effect.as("ok"))),
            {
              "rpc.aggregate": "test",
            },
          ),
        ).pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* TestClock.adjust(duration);
        return yield* Fiber.join(fiber);
      }).pipe(Effect.provide(TestClock.layer()));

      assert.deepStrictEqual(Array.from(events), ["ok"]);

      const snapshots = yield* Metric.snapshot;
      const snapshot = findHistogramSnapshot(snapshots, "t3_rpc_request_duration", {
        method: WS_METHODS.serverGetProcessDiagnostics,
      });

      assert.equal(snapshot?.state.count, 1);
      assert.equal(snapshot?.state.sum, 1.5);
    }),
  );

  it.effect("records failure outcomes when a stream RPC effect produces a failing stream", () =>
    Effect.gen(function* () {
      const exit = yield* Stream.runCollect(
        observeRpcStreamEffect(
          "rpc.instrumentation.stream.effect.failure",
          Effect.succeed(Stream.fail("boom")),
          { "rpc.aggregate": "test" },
        ).pipe(Stream.withSpan("rpc.instrumentation.stream.effect.failure.span")),
      ).pipe(Effect.exit);

      assert.equal(Exit.isFailure(exit), true);

      const snapshots = yield* Metric.snapshot;

      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_requests_total", {
          method: "rpc.instrumentation.stream.effect.failure",
          outcome: "failure",
        }),
        true,
      );
      assert.equal(
        hasMetricSnapshot(snapshots, "t3_rpc_request_duration", {
          method: "rpc.instrumentation.stream.effect.failure",
        }),
        true,
      );
    }),
  );

  it.effect("records spans for traced stream RPC handlers", () =>
    Effect.gen(function* () {
      const spanNames = yield* collectSpanNames(
        Stream.runCollect(
          observeRpcStream(
            "rpc.instrumentation.traced.stream",
            Stream.fromEffect(
              Effect.succeed("ok").pipe(Effect.withSpan("rpc.instrumentation.traced.stream.child")),
            ),
            { "rpc.aggregate": "test" },
          ),
        ),
      );

      assert.equal(spanNames.includes("ws.rpc.rpc.instrumentation.traced.stream"), true);
      assert.equal(spanNames.includes("rpc.instrumentation.traced.stream.child"), true);
    }),
  );

  it.effect("starts a new trace for each traced RPC handler", () =>
    Effect.gen(function* () {
      const spans: Array<Tracer.NativeSpan> = [];
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          spans.push(span);
          return span;
        },
      });

      yield* observeRpcEffect("rpc.instrumentation.root", Effect.void, {
        "rpc.aggregate": "test",
      }).pipe(Effect.withSpan("ws.connection"), Effect.withTracer(tracer));

      const connection = spans.find((span) => span.name === "ws.connection");
      const request = spans.find((span) => span.name === "ws.rpc.rpc.instrumentation.root");
      assert.equal(Option.isNone(request!.parent), true);
      assert.notEqual(request!.traceId, connection!.traceId);
    }),
  );

  it.effect("does not create spans for disabled unary RPC handlers", () =>
    Effect.gen(function* () {
      const spanNames = yield* collectSpanNames(
        observeRpcEffect(
          WS_METHODS.serverGetTraceDiagnostics,
          Effect.succeed("ok").pipe(Effect.withSpan("rpc.instrumentation.disabled.unary.child")),
          { "rpc.aggregate": "test" },
        ),
      );

      assert.deepStrictEqual(spanNames, []);
    }),
  );

  it.effect("does not create spans for disabled direct stream RPC handlers", () =>
    Effect.gen(function* () {
      const spanNames = yield* collectSpanNames(
        Stream.runCollect(
          observeRpcStream(
            WS_METHODS.serverGetTraceDiagnostics,
            Stream.fromEffect(
              Effect.succeed("ok").pipe(
                Effect.withSpan("rpc.instrumentation.disabled.stream.child"),
              ),
            ),
            { "rpc.aggregate": "test" },
          ),
        ),
      );

      assert.deepStrictEqual(spanNames, []);
    }),
  );

  it.effect("parses a valid sentry-trace header, honoring the sampled flag", () =>
    Effect.gen(function* () {
      const traceId = "a".repeat(32);
      const spanId = "b".repeat(16);

      const sampled = externalSpanFromHeaders({ "sentry-trace": `${traceId}-${spanId}-1` });
      assert.equal(sampled?._tag, "ExternalSpan");
      assert.equal(sampled?.traceId, traceId);
      assert.equal(sampled?.spanId, spanId);
      assert.equal(sampled?.sampled, true);

      const unsampled = externalSpanFromHeaders({ "sentry-trace": `${traceId}-${spanId}-0` });
      assert.equal(unsampled?.sampled, false);
    }),
  );

  it.effect("returns undefined for a missing or malformed sentry-trace header", () =>
    Effect.gen(function* () {
      assert.equal(externalSpanFromHeaders({}), undefined);
      assert.equal(externalSpanFromHeaders({ "sentry-trace": "garbage" }), undefined);
    }),
  );

  it.effect("parents a traced RPC on the client's external span when one arrived", () =>
    Effect.gen(function* () {
      const parent = Tracer.externalSpan({
        traceId: "a".repeat(32),
        spanId: "b".repeat(16),
        sampled: true,
      });

      const span = yield* observeRpcEffect("rpc.instrumentation.parented", Effect.currentSpan, {
        "rpc.aggregate": "test",
      }).pipe(Effect.provideService(RpcClientSpan, parent));

      assert.equal(Option.isSome(span.parent), true);
      const spanParent = span.parent as Option.Some<Tracer.AnySpan>;
      assert.equal(spanParent.value._tag, "ExternalSpan");
      assert.equal(spanParent.value.spanId, parent.spanId);
      assert.equal(spanParent.value.traceId, parent.traceId);
    }),
  );

  it.effect("wires the sentry-trace header from middleware options into RpcClientSpan", () =>
    Effect.gen(function* () {
      const middleware = yield* WsClientTraceMiddleware.pipe(
        Effect.provide(clientTraceMiddlewareLayer),
      );

      const traceId = "a".repeat(32);
      const spanId = "b".repeat(16);
      // RpcMiddleware's `SuccessValue` is an opaque marker type effect-smol uses to
      // erase the handler's success type across middleware; there is no real value
      // of that type to hand over, so the inner effect and its result are cast
      // locally rather than threading the marker through the test.
      const seen = (yield* middleware(RpcClientSpan as never, {
        headers: Headers.fromInput({ "sentry-trace": `${traceId}-${spanId}-1` }),
        client: {} as never,
        requestId: "1" as never,
        rpc: {} as never,
        payload: undefined,
      })) as unknown as Tracer.ExternalSpan | undefined;

      assert.equal(seen?._tag, "ExternalSpan");
      assert.equal(seen?.traceId, traceId);
      assert.equal(seen?.spanId, spanId);
    }),
  );

  it.effect("does not create spans for disabled stream effect RPC handlers", () =>
    Effect.gen(function* () {
      const spanNames = yield* collectSpanNames(
        Stream.runCollect(
          observeRpcStreamEffect(
            WS_METHODS.serverGetTraceDiagnostics,
            Effect.succeed(
              Stream.fromEffect(
                Effect.succeed("ok").pipe(
                  Effect.withSpan("rpc.instrumentation.disabled.stream.effect.consume"),
                ),
              ),
            ).pipe(Effect.withSpan("rpc.instrumentation.disabled.stream.effect.create")),
            { "rpc.aggregate": "test" },
          ),
        ),
      );

      assert.deepStrictEqual(spanNames, []);
    }),
  );
});
