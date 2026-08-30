import * as Sentry from "@sentry/react";
import type { AnyRouter } from "@tanstack/react-router";
import type { RootOptions } from "react-dom/client";

const pairingTokenPattern = /([?#&]token=)[^&#\s"']+/g;

/** Replaces any pairing token in a URL-like string with Sentry's `[Filtered]` marker. */
export const scrubPairingToken = (value: string): string =>
  value.replace(pairingTokenPattern, "$1[Filtered]");

/**
 * Walks an event and scrubs pairing tokens from every string it carries. The
 * pairing page keeps `#token=` in the URL until the user submits, and request
 * URLs, breadcrumbs, and transaction names all capture `location.href`.
 */
export const scrubPairingTokens = <T>(value: T): T => {
  if (typeof value === "string") return scrubPairingToken(value) as T;
  if (Array.isArray(value)) return value.map(scrubPairingTokens) as T;
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [key, entry] of Object.entries(value)) out[key] = scrubPairingTokens(entry);
    return out as T;
  }
  return value;
};

/** Enables Sentry when VITE_SENTRY_DSN is set at build time; no-op otherwise. */
export function initSentry(router: AnyRouter): void {
  const dsn = import.meta.env.VITE_SENTRY_DSN?.trim();
  if (!dsn) return;
  Sentry.init({
    dsn,
    environment: import.meta.env.MODE,
    integrations: [Sentry.tanstackRouterBrowserTracingIntegration(router)],
    tracesSampleRate: 1,
    beforeSend: scrubPairingTokens,
    beforeSendTransaction: scrubPairingTokens,
    beforeBreadcrumb: scrubPairingTokens,
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
