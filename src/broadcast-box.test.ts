import { expect, test } from "bun:test";
import { BroadcastBoxClient } from "./broadcast-box.ts";
import { config } from "./config.ts";
import { logger } from "./logger.ts";

logger.level = 0; // keep test output quiet

// Only the API URL is dynamic per test (a fresh loopback server); everything
// else comes from the shared `config`.
function configForUrl(apiUrl: string) {
  return { ...config, broadcastBox: { ...config.broadcastBox, apiUrl } };
}

test("fetchLiveStreamKeys sends a base64 bearer and filters to live publishers", async () => {
  let seenAuth = "";
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      seenAuth = request.headers.get("authorization") ?? "";
      return Response.json([
        { streamKey: "azn", streamStart: 1_720_000_000, videoTracks: [{ rid: "h" }] },
        { streamKey: "vieweronly", streamStart: 0, videoTracks: [], audioTracks: [] },
        { streamKey: "audioonly", audioTracks: [{ rid: "a" }] },
        { streamKey: "", streamStart: 123 },
      ]);
    },
  });

  const client = new BroadcastBoxClient(configForUrl(server.url.origin));
  const live = await client.fetchLiveStreamKeys();
  server.stop(true);

  expect(seenAuth).toBe(`Bearer ${btoa("secret")}`);
  expect([...live].sort()).toEqual(["audioonly", "azn"]);
});

test("throws on a non-2xx response", async () => {
  const server = Bun.serve({ port: 0, fetch: () => new Response("nope", { status: 401 }) });
  const client = new BroadcastBoxClient(configForUrl(server.url.origin));

  await expect(client.fetchLiveStreamKeys()).rejects.toThrow("401");
  server.stop(true);
});
