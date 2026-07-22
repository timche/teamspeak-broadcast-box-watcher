import { expect, test } from "bun:test";
import { loadConfig } from "../src/config.ts";
import { createLogger } from "../src/logger.ts";
import type { TemporaryGroup } from "../src/teamspeak.ts";
import { Watcher } from "../src/watcher.ts";

const config = loadConfig({
  BROADCAST_BOX_API_URL: "http://broadcast-box:8080",
  BROADCAST_BOX_ADMIN_TOKEN: "secret",
  PUBLIC_STREAM_HOST: "https://stream.example.com/",
  TEAMSPEAK_HOST: "teamspeak",
  TEAMSPEAK_QUERY_PASSWORD: "pw",
  LOG_LEVEL: "error",
});
const logger = createLogger(config.logLevel);

function makeTeamspeak(
  existing: TemporaryGroup[],
  clients: { nickname: string; databaseId: string }[],
) {
  const created: string[] = [];
  const assigned: [string, string][] = [];
  const deleted: string[] = [];
  let clientFetches = 0;
  let groups = [...existing];

  const ts = {
    listClients: async () => {
      clientFetches++;
      return clients;
    },
    listTemporaryGroups: async () => groups,
    createGroupAndAssign: async (name: string, dbid: string) => {
      created.push(name);
      assigned.push([name, dbid]);
      groups.push({ sgid: `new-${name}`, name });
    },
    deleteGroup: async (group: TemporaryGroup) => {
      deleted.push(group.name);
      groups = groups.filter((existingGroup) => existingGroup.sgid !== group.sgid);
    },
    disconnect: async () => undefined,
  };

  return {
    ts,
    created,
    assigned,
    deleted,
    clientFetches: () => clientFetches,
  };
}

test("config base64-encodes the cleartext token and normalizes the public host", () => {
  expect(config.broadcastBox.authorization).toBe(`Bearer ${btoa("secret")}`);
  expect(config.publicStreamHost).toBe("stream.example.com");
});

test("go-live: creates and assigns a group for a matching client", async () => {
  const broadcastBox = { fetchLiveStreamKeys: async () => new Set(["azn"]) };
  const { ts, created, assigned, deleted } = makeTeamspeak(
    [],
    [{ nickname: "AzN", databaseId: "42" }],
  );

  await new Watcher(config, logger, broadcastBox as never, ts as never).reconcile();

  expect(created).toEqual(["🔴 stream.example.com/azn"]);
  expect(assigned).toEqual([["🔴 stream.example.com/azn", "42"]]);
  expect(deleted).toEqual([]);
});

test("still-live: existing group is left untouched (no churn)", async () => {
  const broadcastBox = { fetchLiveStreamKeys: async () => new Set(["azn"]) };
  const existing = [{ sgid: "1", name: "🔴 stream.example.com/azn" }];
  const { ts, created, deleted } = makeTeamspeak(existing, [{ nickname: "azn", databaseId: "42" }]);

  await new Watcher(config, logger, broadcastBox as never, ts as never).reconcile();

  expect(created).toEqual([]);
  expect(deleted).toEqual([]);
});

test("go-offline: deletes a temp group whose stream ended", async () => {
  const broadcastBox = { fetchLiveStreamKeys: async () => new Set(["stillup"]) };
  const existing = [
    { sgid: "1", name: "🔴 stream.example.com/ended" },
    { sgid: "2", name: "🔴 stream.example.com/stillup" },
  ];
  const { ts, deleted, created } = makeTeamspeak(existing, [
    { nickname: "stillup", databaseId: "7" },
  ]);

  await new Watcher(config, logger, broadcastBox as never, ts as never).reconcile();

  expect(deleted).toEqual(["🔴 stream.example.com/ended"]);
  expect(created).toEqual([]);
});

test("no streams: cleans up all temp groups and skips the client fetch", async () => {
  const broadcastBox = { fetchLiveStreamKeys: async () => new Set<string>() };
  const existing = [{ sgid: "1", name: "🔴 stream.example.com/old" }];
  const { ts, deleted, clientFetches } = makeTeamspeak(existing, []);

  await new Watcher(config, logger, broadcastBox as never, ts as never).reconcile();

  expect(deleted).toEqual(["🔴 stream.example.com/old"]);
  expect(clientFetches()).toBe(0);
});

test("live stream with no matching TeamSpeak user creates nothing", async () => {
  const broadcastBox = { fetchLiveStreamKeys: async () => new Set(["ghost"]) };
  const { ts, created } = makeTeamspeak([], [{ nickname: "someoneelse", databaseId: "9" }]);

  await new Watcher(config, logger, broadcastBox as never, ts as never).reconcile();

  expect(created).toEqual([]);
});
