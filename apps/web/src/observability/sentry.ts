import * as Sentry from "@sentry/react";
import type { AnyRouter } from "@tanstack/react-router";
import type { RootOptions } from "react-dom/client";

/** Enables Sentry when VITE_SENTRY_DSN is set at build time; no-op otherwise. */
export function initSentry(router: AnyRouter): void {
  const dsn = import.meta.env.VITE_SENTRY_DSN?.trim();
  if (!dsn) return;
  Sentry.init({
    dsn,
    environment: import.meta.env.MODE,
    integrations: [Sentry.tanstackRouterBrowserTracingIntegration(router)],
    tracesSampleRate: 1,
  });
}

const reportReactError = Sentry.reactErrorHandler();

// react-dom types componentStack as optional; Sentry's handler wants string | null.
const onReactError: RootOptions["onUncaughtError"] = (error, errorInfo) =>
  reportReactError(error, { componentStack: errorInfo.componentStack ?? null });

/** Root options that report React render errors to Sentry (safe when Sentry is off). */
export const sentryRootOptions: RootOptions = {
  onUncaughtError: onReactError,
  onCaughtError: onReactError,
  onRecoverableError: onReactError,
};
