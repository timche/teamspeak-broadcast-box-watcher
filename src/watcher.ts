import type { BroadcastBoxClient } from "./broadcast-box.ts";
import type { Config } from "./config.ts";
import type { Logger } from "./logger.ts";
import type { TeamSpeakManager, TemporaryGroup } from "./teamspeak.ts";

/**
 * Reconciles TeamSpeak temporary groups against the set of users currently
 * live on Broadcast Box. The watcher keeps no in-memory state: every poll the
 * desired state is diffed against the groups that actually exist on the
 * TeamSpeak server, so cleanup happens on every cycle and restarts/crashes
 * recover automatically.
 */
export class Watcher {
  readonly #config: Config;
  readonly #logger: Logger;
  readonly #broadcastBox: BroadcastBoxClient;
  readonly #teamspeak: TeamSpeakManager;

  constructor(
    config: Config,
    logger: Logger,
    broadcastBox: BroadcastBoxClient,
    teamspeak: TeamSpeakManager,
  ) {
    this.#config = config;
    this.#logger = logger;
    this.#broadcastBox = broadcastBox;
    this.#teamspeak = teamspeak;
  }

  /** Group name for a stream, e.g. `🔴 stream.example.com/azn`. */
  #groupName(streamKey: string): string {
    return `${this.#config.groupPrefix} ${this.#config.publicStreamHost}/${streamKey}`;
  }

  /** Runs a single reconciliation cycle. */
  async reconcile(signal?: AbortSignal): Promise<void> {
    const liveStreamKeys = await this.#broadcastBox.fetchLiveStreamKeys(signal);
    const existingGroups = await this.#teamspeak.listTemporaryGroups();

    // Nothing is live: tear down any leftover temp groups and skip the
    // (larger) client list entirely.
    if (liveStreamKeys.size === 0) {
      await this.#deleteGroups(existingGroups);

      return;
    }

    const clients = await this.#teamspeak.listClients();
    const clientByNickname = new Map<string, string>();
    for (const client of clients) {
      clientByNickname.set(client.nickname.toLowerCase(), client.databaseId);
    }

    // Desired state: one group per live stream that maps to a connected client.
    const desiredNames = new Map<string, { streamKey: string; databaseId: string }>();
    for (const streamKey of liveStreamKeys) {
      const databaseId = clientByNickname.get(streamKey.toLowerCase());

      if (databaseId === undefined) {
        this.#logger.debug(`Live stream "${streamKey}" has no matching connected TeamSpeak user`);
        continue;
      }

      desiredNames.set(this.#groupName(streamKey), { streamKey, databaseId });
    }

    // Cleanup: delete existing temp groups that are no longer desired.
    const existingNames = new Set<string>();
    const stale: TemporaryGroup[] = [];
    for (const group of existingGroups) {
      existingNames.add(group.name);

      if (!desiredNames.has(group.name)) {
        stale.push(group);
      }
    }
    await this.#deleteGroups(stale);

    // Create: add groups for newly live streamers, leaving existing ones intact.
    for (const [name, { streamKey, databaseId }] of desiredNames) {
      if (existingNames.has(name)) {
        continue;
      }

      try {
        await this.#teamspeak.createGroupAndAssign(name, databaseId);
      } catch (error) {
        this.#logger.error(
          `Failed to create/assign group for stream "${streamKey}":`,
          errorMessage(error),
        );
      }
    }
  }

  async #deleteGroups(groups: TemporaryGroup[]): Promise<void> {
    for (const group of groups) {
      try {
        await this.#teamspeak.deleteGroup(group);
      } catch (error) {
        this.#logger.error(`Failed to delete group "${group.name}":`, errorMessage(error));
      }
    }
  }
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
