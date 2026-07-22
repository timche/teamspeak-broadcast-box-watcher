import { expect, test } from "bun:test";
import { BroadcastBoxClient } from "./broadcast-box.ts";
import { logger } from "./logger.ts";

logger.level = 0; // keep test output quiet

function clientForUrl(apiUrl: string) {
  return new BroadcastBoxClient({ apiUrl, authorization: `Bearer ${btoa("s3cr3t")}` });
}

test("fetchLiveStreamKeys sends the bearer and filters to live publishers", async () => {
  let seenAuth = "";
  const server = Bun.serve({
    port: 0,
    fetch(request) {
      seenAuth = request.headers.get("authorization") ?? "";
      return Response.json([
        {
          streamKey: "azn",
          streamStart: "2026-07-22T13:29:24.454898621Z",
          audioTracks: [{ rid: "Audio" }],
          videoTracks: [{ rid: "WebLow" }],
          sessions: [],
        },
        {
          // Offline: a provisioned stream key with a start time but no tracks.
          streamKey: "offline",
          streamStart: "2026-07-22T13:28:42.534422488Z",
          audioTracks: [],
          videoTracks: [],
          sessions: [{ id: "c615e1c9" }],
        },
        { streamKey: "audioonly", audioTracks: [{ rid: "a" }] },
        { streamKey: "", audioTracks: [{ rid: "a" }] },
      ]);
    },
  });

  const live = await clientForUrl(server.url.origin).fetchLiveStreamKeys();
  server.stop(true);

  expect(seenAuth).toBe(`Bearer ${btoa("s3cr3t")}`);
  expect([...live].sort()).toEqual(["audioonly", "azn"]);
});

test("treats a null status body (no streams) as empty", async () => {
  const server = Bun.serve({ port: 0, fetch: () => Response.json(null) });

  const live = await clientForUrl(server.url.origin).fetchLiveStreamKeys();
  server.stop(true);

  expect([...live]).toEqual([]);
});

test("throws on a non-2xx response", async () => {
  const server = Bun.serve({ port: 0, fetch: () => new Response("nope", { status: 401 }) });
  const client = clientForUrl(server.url.origin);

  await expect(client.fetchLiveStreamKeys()).rejects.toThrow("401");
  server.stop(true);
});
