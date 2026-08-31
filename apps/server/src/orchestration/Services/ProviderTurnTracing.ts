/**
 * ProviderTurnTracing - Provider-agnostic gen_ai span reactor service interface.
 *
 * Turns provider runtime turn events into gen_ai spans so agent runs show up
 * in tracing backends (Sentry AI Agents, OTLP) regardless of provider.
 *
 * @module ProviderTurnTracing
 */
import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

/**
 * ProviderTurnTracingShape - Service API for provider turn tracing.
 */
export interface ProviderTurnTracingShape {
  /**
   * Start reacting to provider runtime events, producing gen_ai spans.
   *
   * The returned effect must be run in a scope so the worker fiber can be
   * finalized on shutdown.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;
}

/**
 * ProviderTurnTracing - Service tag for the gen_ai span reactor.
 */
export class ProviderTurnTracing extends Context.Service<
  ProviderTurnTracing,
  ProviderTurnTracingShape
>()("t3/orchestration/Services/ProviderTurnTracing") {}
