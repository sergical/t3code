/**
 * Derives Sentry's OTLP trace-ingest endpoint from a DSN, for exporting
 * traces to Sentry through the generic OTLP tracer instead of the Sentry
 * SDK. See https://docs.sentry.io/concepts/otlp/ for the endpoint shape.
 * Returns undefined when the DSN does not parse.
 */
export const sentryOtlpTraces = (
  dsn: string,
): { readonly url: string; readonly headers: Record<string, string> } | undefined => {
  let parsed: URL;
  try {
    parsed = new URL(dsn);
  } catch {
    return undefined;
  }
  const publicKey = parsed.username;
  const projectId = parsed.pathname.replace(/^\/+/, "");
  if (publicKey === "" || !/^\d+$/.test(projectId)) return undefined;
  return {
    url: `${parsed.protocol}//${parsed.host}/api/${projectId}/integration/otlp/v1/traces`,
    headers: { "x-sentry-auth": `sentry sentry_key=${publicKey}` },
  };
};
