import * as Sentry from "@sentry/effect/server";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Tracer from "effect/Tracer";

import { makeSentryTracer } from "./Sentry.ts";

const traceId = "a".repeat(32);
const requestSpanId = "b".repeat(16);

describe("makeSentryTracer", () => {
  it.live("continued traces carry a dynamic sampling context", () =>
    Effect.gen(function* () {
      const tracer = yield* makeSentryTracer({
        mode: "web",
        sentryDsn: "https://publickey@o1.ingest.sentry.io/1",
      });
      const client = Sentry.getClient();
      assert.notEqual(client, undefined);
      if (client === undefined) return;
      const captured = new Promise<Sentry.Event>((resolve) => {
        client.getOptions().beforeSendTransaction = (event) => {
          resolve(event);
          return null;
        };
      });

      yield* Effect.gen(function* () {
        const turn = yield* Effect.makeSpan("invoke_agent claude", {
          parent: Tracer.externalSpan({
            traceId,
            spanId: requestSpanId,
            sampled: true,
          }),
        });
        const chat = yield* Effect.makeSpan("chat claude", {
          parent: turn,
          attributes: { "sentry.op": "gen_ai.chat" },
        });
        const now = yield* Clock.currentTimeNanos;
        chat.end(now, Exit.void);
        turn.end(now, Exit.void);
      }).pipe(Effect.withTracer(tracer));

      const transaction = yield* Effect.promise(() => captured);
      assert.equal(transaction.contexts?.trace?.parent_span_id, requestSpanId);
      assert.deepEqual(
        transaction.spans?.map((span) => [span.description, span.op]),
        [["chat claude", "gen_ai.chat"]],
      );
      assert.equal(transaction.contexts?.trace?.op, "function");
      const dsc = transaction.sdkProcessingMetadata?.dynamicSamplingContext;
      assert.equal(dsc?.trace_id, traceId);
      assert.equal(dsc?.public_key, "publickey");
      assert.equal(dsc?.sampled, "true");
    }).pipe(Effect.scoped),
  );
});
