import { describe, expect, it } from "@effect/vitest";

import { sentryOtlpTraces } from "./sentryOtlp.ts";

describe("sentryOtlpTraces", () => {
  it("derives the OTLP trace endpoint and auth header from a DSN", () => {
    expect(sentryOtlpTraces("https://abc123@o450.ingest.us.sentry.io/4509")).toEqual({
      url: "https://o450.ingest.us.sentry.io/api/4509/integration/otlp/v1/traces",
      headers: { "x-sentry-auth": "sentry sentry_key=abc123" },
    });
  });

  it("rejects a DSN without a public key or numeric project id", () => {
    expect(sentryOtlpTraces("https://o450.ingest.us.sentry.io/4509")).toBeUndefined();
    expect(sentryOtlpTraces("https://abc@o450.ingest.us.sentry.io/")).toBeUndefined();
    expect(sentryOtlpTraces("not a dsn")).toBeUndefined();
  });
});
