import { describe, expect, it } from "vitest";

import { scrubPairingTokens } from "./sentry";

describe("scrubPairingTokens", () => {
  it("filters the pairing token wherever an event captured the page URL", () => {
    const event = {
      request: { url: "http://localhost:5733/pair#token=EAMUMVX3MSCY" },
      transaction: "/pair?token=EAMUMVX3MSCY&next=/",
      breadcrumbs: [{ data: { to: "/pair#token=EAMUMVX3MSCY" } }],
      contexts: { trace: { op: "pageload" } },
    };

    expect(scrubPairingTokens(event)).toEqual({
      request: { url: "http://localhost:5733/pair#token=[Filtered]" },
      transaction: "/pair?token=[Filtered]&next=/",
      breadcrumbs: [{ data: { to: "/pair#token=[Filtered]" } }],
      contexts: { trace: { op: "pageload" } },
    });
  });
});
