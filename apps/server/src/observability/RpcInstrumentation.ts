import { WS_METHODS, WsClientTraceMiddleware } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as References from "effect/References";
import * as Stream from "effect/Stream";
import * as Tracer from "effect/Tracer";

import { outcomeFromExit } from "./Attributes.ts";
import { metricAttributes, rpcRequestDuration, rpcRequestsTotal, withMetrics } from "./Metrics.ts";

const RPC_SPAN_PREFIX = "ws.rpc";
const DEFAULT_RPC_SPAN_ATTRIBUTES = {
  "rpc.transport": "websocket",
  "rpc.system": "effect-rpc",
} as const;
const RPC_METHODS_WITH_TRACING_DISABLED: ReadonlySet<string> = new Set([
  WS_METHODS.serverGetTraceDiagnostics,
  WS_METHODS.serverGetProcessDiagnostics,
  WS_METHODS.serverGetProcessResourceHistory,
  WS_METHODS.serverSignalProcess,
]);

function shouldTraceRpc(method: string): boolean {
  return !RPC_METHODS_WITH_TRACING_DISABLED.has(method);
}

const rpcSpanAttributes = (
  method: string,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Record<string, unknown> => ({
  ...DEFAULT_RPC_SPAN_ATTRIBUTES,
  "rpc.method": method,
  ...traceAttributes,
});

const rpcSpanOptions = (
  method: string,
  parent: Tracer.ExternalSpan | undefined,
  traceAttributes?: Readonly<Record<string, unknown>>,
) => ({
  ...(parent === undefined ? { root: true } : { parent }),
  attributes: rpcSpanAttributes(method, traceAttributes),
});

/**
 * The trace context a client sent with the request, decoded from its
 * sentry-trace header. Absent for clients that do not run a tracing SDK.
 */
export const RpcClientSpan = Context.Reference<Tracer.ExternalSpan | undefined>(
  "apps/server/observability/RpcClientSpan",
  { defaultValue: () => undefined },
);

const SENTRY_TRACE_PATTERN = /^([0-9a-f]{32})-([0-9a-f]{16})(?:-([01]))?$/;

export const externalSpanFromHeaders = (
  headers: Readonly<Record<string, string | undefined>>,
): Tracer.ExternalSpan | undefined => {
  const match = headers["sentry-trace"]?.match(SENTRY_TRACE_PATTERN);
  if (match === null || match === undefined) return undefined;
  return Tracer.externalSpan({
    traceId: match[1]!,
    spanId: match[2]!,
    sampled: match[3] !== "0",
  });
};

export const clientTraceMiddlewareLayer = Layer.succeed(
  WsClientTraceMiddleware,
  WsClientTraceMiddleware.of((effect, { headers }) => {
    const parent = externalSpanFromHeaders(headers);
    return parent === undefined ? effect : Effect.provideService(effect, RpcClientSpan, parent);
  }),
);

// A request from a traced client joins that client's trace; its sentry-trace
// header arrives through the group middleware as RpcClientSpan. Every other
// request is its own trace root: the WebSocket connection span above it lives
// as long as the socket, and exporters that ship a tree only when its root
// ends would never send a request that stays parented on it.
const withRpcEffectTracing = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Effect.Effect<A, E, R> =>
  shouldTraceRpc(method)
    ? RpcClientSpan.pipe(
        Effect.flatMap((parent) =>
          effect.pipe(
            Effect.withSpan(
              `${RPC_SPAN_PREFIX}.${method}`,
              rpcSpanOptions(method, parent, traceAttributes),
            ),
          ),
        ),
      )
    : effect.pipe(Effect.provideService(References.TracerEnabled, false));

const withRpcStreamTracing = <A, E, R>(
  method: string,
  stream: Stream.Stream<A, E, R>,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Stream.Stream<A, E, R> =>
  shouldTraceRpc(method)
    ? Stream.unwrap(
        Effect.map(RpcClientSpan, (parent) =>
          stream.pipe(
            Stream.withSpan(
              `${RPC_SPAN_PREFIX}.${method}`,
              rpcSpanOptions(method, parent, traceAttributes),
            ),
          ),
        ),
      )
    : stream.pipe(Stream.provideService(References.TracerEnabled, false));

const recordRpcStreamMetrics = <E>(
  method: string,
  startedAt: bigint,
  exit: Exit.Exit<unknown, E>,
): Effect.Effect<void, never, never> =>
  Effect.gen(function* () {
    const endedAt = yield* Clock.currentTimeNanos;
    const elapsedNanos = endedAt > startedAt ? endedAt - startedAt : 0n;

    yield* Metric.update(
      Metric.withAttributes(rpcRequestDuration, metricAttributes({ method })),
      Duration.nanos(elapsedNanos),
    );
    yield* Metric.update(
      Metric.withAttributes(
        rpcRequestsTotal,
        metricAttributes({
          method,
          outcome: outcomeFromExit(exit),
        }),
      ),
      1,
    );
  });

export const observeRpcEffect = <A, E, R>(
  method: string,
  effect: Effect.Effect<A, E, R>,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Effect.Effect<A, E, R> => {
  const instrumented = effect.pipe(
    withMetrics({
      counter: rpcRequestsTotal,
      timer: rpcRequestDuration,
      attributes: {
        method,
      },
    }),
  );

  return withRpcEffectTracing(method, instrumented, traceAttributes);
};

export const observeRpcStream = <A, E, R>(
  method: string,
  stream: Stream.Stream<A, E, R>,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Stream.Stream<A, E, R> => {
  const instrumented = Stream.unwrap(
    Effect.gen(function* () {
      const startedAt = yield* Clock.currentTimeNanos;
      return stream.pipe(Stream.onExit((exit) => recordRpcStreamMetrics(method, startedAt, exit)));
    }),
  );

  return withRpcStreamTracing(method, instrumented, traceAttributes);
};

export const observeRpcStreamEffect = <A, StreamError, StreamContext, EffectError, EffectContext>(
  method: string,
  effect: Effect.Effect<Stream.Stream<A, StreamError, StreamContext>, EffectError, EffectContext>,
  traceAttributes?: Readonly<Record<string, unknown>>,
): Stream.Stream<A, StreamError | EffectError, StreamContext | EffectContext> => {
  const instrumented = Stream.unwrap(
    Effect.gen(function* () {
      const startedAt = yield* Clock.currentTimeNanos;
      const exit = yield* Effect.exit(effect);

      if (Exit.isFailure(exit)) {
        yield* recordRpcStreamMetrics(method, startedAt, exit);
        return yield* Effect.failCause(exit.cause);
      }

      return exit.value.pipe(
        Stream.onExit((streamExit) => recordRpcStreamMetrics(method, startedAt, streamExit)),
      );
    }),
  );

  return withRpcStreamTracing(method, instrumented, traceAttributes);
};
