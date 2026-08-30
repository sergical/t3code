import * as Sentry from "@sentry/effect/server";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import type * as Scope from "effect/Scope";
import * as Tracer from "effect/Tracer";

import type { ServerConfig } from "../../config.ts";

/**
 * Initializes the Sentry SDK and returns an Effect tracer that forwards every
 * span to it. Used as the delegate of the local file tracer so existing
 * `Effect.withSpan` call sites need no changes. Flushes pending events when
 * the owning scope closes.
 *
 * Every parentless span starts a new Sentry trace. Without this, a long-lived
 * Node process reuses one propagation context and every request and agent turn
 * lands in a single trace for the lifetime of the server.
 *
 * A span whose parent is an external span (a persisted event's trace, or a
 * client's sentry-trace header) is opened through `continueTrace`, so reactor
 * work and agent turns land in the trace of the request that caused them.
 */
/**
 * Reports an agent turn that ends in failure as a Sentry issue. Interrupted
 * turns (user stop, cancel) are not failures. The issue is captured with the
 * turn's span active so Sentry links it to the trace.
 */
const captureFailedTurn = (span: Tracer.Span) => {
  const end = span.end.bind(span);
  span.end = (endTime, exit) => {
    end(endTime, exit);
    const attributes = span.attributes;
    if (attributes.get("sentry.op") !== "gen_ai.invoke_agent") return;
    if (!Exit.isFailure(exit) || Cause.hasInterruptsOnly(exit.cause)) return;
    const error = new Error(String(Cause.squash(exit.cause)));
    error.name = "AgentTurnFailed";
    const capture = () =>
      Sentry.captureException(error, {
        tags: {
          "t3.thread.id": String(attributes.get("t3.thread.id")),
          "t3.turn.id": String(attributes.get("t3.turn.id")),
          "gen_ai.agent.name": String(attributes.get("gen_ai.agent.name")),
        },
      });
    const { sentrySpan } = span as { sentrySpan?: Sentry.Span };
    if (sentrySpan === undefined) capture();
    else Sentry.withActiveSpan(sentrySpan, capture);
  };
  return span;
};

export const makeSentryTracer = Effect.fn("makeSentryTracer")(function* (
  config: Pick<ServerConfig["Service"], "mode"> & { readonly sentryDsn: string },
): Effect.fn.Return<Tracer.Tracer, never, Scope.Scope> {
  Sentry.init({
    dsn: config.sentryDsn,
    environment: config.mode,
    tracesSampleRate: 1,
  });
  yield* Effect.addFinalizer(() => Effect.promise(() => Sentry.flush(2_000)).pipe(Effect.ignore));
  const sentryTracer = Sentry.SentryEffectTracer;
  return Tracer.make({
    span: (options) => {
      const open = () => captureFailedTurn(sentryTracer.span(options));
      const parent = options.parent;
      if (Option.isNone(parent)) return Sentry.startNewTrace(open);
      if (parent.value._tag === "ExternalSpan") {
        const { traceId, spanId, sampled } = parent.value;
        return Sentry.continueTrace(
          { sentryTrace: `${traceId}-${spanId}-${sampled ? "1" : "0"}`, baggage: undefined },
          open,
        );
      }
      return open();
    },
    context: sentryTracer.context,
  });
});
