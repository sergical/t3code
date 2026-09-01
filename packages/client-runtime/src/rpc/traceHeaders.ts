import * as Context from "effect/Context";

/**
 * Optional per-client source of trace propagation headers. A client that runs
 * a tracing SDK (the web app's Sentry init) provides one so every outgoing RPC
 * request carries its current trace context; clients without tracing provide
 * nothing and requests go out unchanged.
 */
export class RpcTraceHeaders extends Context.Service<
  RpcTraceHeaders,
  {
    readonly current: () => Readonly<Record<string, string>>;
  }
>()("@t3tools/client-runtime/rpc/traceHeaders/RpcTraceHeaders") {}
