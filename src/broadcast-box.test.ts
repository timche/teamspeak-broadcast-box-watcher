import { afterEach, expect, test } from "bun:test";
import { BroadcastBoxClient } from "./broadcast-box.ts";
import { logger } from "./logger.ts";

logger.level = 0; // keep test output quiet

const realFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = realFetch;
});

/** Replaces `fetch` with a stub, capturing the request and returning `responder()`. */
function stubFetch(responder: () => Response): { request: () => Request } {
  let captured: Request | undefined;

  globalThis.fetch = ((...[input, init]: Parameters<typeof fetch>) => {
    captured = input instanceof Request ? input : new Request(String(input), init);

    return Promise.resolve(responder());
  }) as typeof fetch;

  return {
    request: () => {
      if (captured === undefined) {
        throw new Error("fetch was not called");
      }

      return captured;
    },
  };
}

test("fetchLiveStreamKeys sends a base64 bearer and filters to live publishers", async () => {
  const stub = stubFetch(() =>
    Response.json([
      { streamKey: "azn", streamStart: 1_720_000_000, videoTracks: [{ rid: "h" }] },
      { streamKey: "vieweronly", streamStart: 0, videoTracks: [], audioTracks: [] },
      { streamKey: "audioonly", audioTracks: [{ rid: "a" }] },
      { streamKey: "", streamStart: 123 },
    ]),
  );

  const live = await new BroadcastBoxClient().fetchLiveStreamKeys();

  expect(stub.request().headers.get("authorization")).toBe(`Bearer ${btoa("secret")}`);
  expect([...live].sort()).toEqual(["audioonly", "azn"]);
});

test("throws on a non-2xx response", async () => {
  stubFetch(() => new Response("nope", { status: 401 }));

  await expect(new BroadcastBoxClient().fetchLiveStreamKeys()).rejects.toThrow("401");
});
