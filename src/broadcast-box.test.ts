import { expect, test } from "bun:test";
import { BroadcastBoxClient } from "./broadcast-box.ts";
import { configSchema } from "./config.ts";
import { logger } from "./logger.ts";

logger.level = 0; // keep test output quiet

function configForUrl(apiUrl: string) {
  return configSchema.parse({
    BROADCAST_BOX_API_URL: apiUrl,
    BROADCAST_BOX_ADMIN_TOKEN: "s3cr3t",
    PUBLIC_STREAM_HOST: "stream.example.com",
    TEAMSPEAK_HOST: "ts",
    TEAMSPEAK_QUERY_PASSWORD: "pw",
    LOG_LEVEL: "error",
  });
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

  expect(seenAuth).toBe(`Bearer ${btoa("s3cr3t")}`);
  expect([...live].sort()).toEqual(["audioonly", "azn"]);
});

test("throws on a non-2xx response", async () => {
  const server = Bun.serve({ port: 0, fetch: () => new Response("nope", { status: 401 }) });
  const client = new BroadcastBoxClient(configForUrl(server.url.origin));

  await expect(client.fetchLiveStreamKeys()).rejects.toThrow("401");
  server.stop(true);
});
