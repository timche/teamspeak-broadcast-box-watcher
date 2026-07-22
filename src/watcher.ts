import type { BroadcastBoxClient } from "./broadcast-box.ts";
import { logger } from "./logger.ts";
import type { ServerGroupRef, TeamSpeakManager } from "./teamspeak.ts";

/**
 * Reconciles TeamSpeak groups against the users currently live on Broadcast Box.
 *
 * Every live user gets two things:
 *   1. membership in the shared "live" group (shown before the nickname);
 *   2. an individual group named after their stream link (visible in their
 *      server-group list).
 *
 * The watcher keeps no in-memory state — each poll diffs the desired state
 * against what actually exists on the server, so it self-heals across restarts.
 */
export class Watcher {
  readonly #broadcastBox: BroadcastBoxClient;
  readonly #teamspeak: TeamSpeakManager;
  readonly #liveGroupSgid: string;
  readonly #streamGroupPrefix: string;
  readonly #publicStreamHost: string;

  constructor(
    broadcastBox: BroadcastBoxClient,
    teamspeak: TeamSpeakManager,
    liveGroupSgid: string,
    options: { streamGroupPrefix: string; publicStreamHost: string },
  ) {
    this.#broadcastBox = broadcastBox;
    this.#teamspeak = teamspeak;
    this.#liveGroupSgid = liveGroupSgid;
    this.#streamGroupPrefix = options.streamGroupPrefix;
    this.#publicStreamHost = options.publicStreamHost;
  }

  /** Per-user stream-link group name, e.g. `🔴 stream.example.com/alice`. */
  #streamGroupName(streamKey: string): string {
    return `${this.#streamGroupPrefix} ${this.#publicStreamHost}/${streamKey}`;
  }

  /** Name prefix used to find the per-user stream-link groups. */
  #streamGroupNamePrefix(): string {
    return `${this.#streamGroupPrefix} `;
  }

  /** Runs a single reconciliation cycle. */
  async reconcile(signal?: AbortSignal): Promise<void> {
    const liveStreamKeys = await this.#broadcastBox.fetchLiveStreamKeys(signal);
    const currentMembers = await this.#teamspeak.listGroupMemberDbids(this.#liveGroupSgid);
    const existingStreamGroups = await this.#teamspeak.listGroupsByPrefix(
      this.#streamGroupNamePrefix(),
      this.#liveGroupSgid,
    );

    // Nothing is live: clear the shared group and all per-user groups, and skip
    // the (larger) client list entirely.
    if (liveStreamKeys.size === 0) {
      await this.#removeMembers(currentMembers);
      await this.#deleteGroups(existingStreamGroups);

      return;
    }

    const clients = await this.#teamspeak.listClients();
    const databaseIdByNickname = new Map<string, string>();
    for (const client of clients) {
      databaseIdByNickname.set(client.nickname.toLowerCase(), client.databaseId);
    }

    // Desired state for the live users that map to a connected client.
    const desiredMembers = new Set<string>();
    const desiredStreamGroups = new Map<string, { streamKey: string; databaseId: string }>();
    for (const streamKey of liveStreamKeys) {
      const databaseId = databaseIdByNickname.get(streamKey.toLowerCase());

      if (databaseId === undefined) {
        logger.debug(`Live stream "${streamKey}" has no matching connected TeamSpeak user`);
        continue;
      }

      desiredMembers.add(databaseId);
      desiredStreamGroups.set(this.#streamGroupName(streamKey), { streamKey, databaseId });
    }

    await this.#reconcileSharedMembership(currentMembers, desiredMembers);
    await this.#reconcileStreamGroups(existingStreamGroups, desiredStreamGroups);
  }

  /** Best-effort teardown for shutdown: empty the shared group, delete per-user groups. */
  async cleanup(): Promise<void> {
    const members = await this.#teamspeak.listGroupMemberDbids(this.#liveGroupSgid);
    await this.#removeMembers(members);

    const groups = await this.#teamspeak.listGroupsByPrefix(
      this.#streamGroupNamePrefix(),
      this.#liveGroupSgid,
    );
    await this.#deleteGroups(groups);
  }

  async #reconcileSharedMembership(current: Set<string>, desired: Set<string>): Promise<void> {
    for (const databaseId of desired) {
      if (current.has(databaseId)) {
        continue;
      }

      try {
        await this.#teamspeak.addClientToGroup(databaseId, this.#liveGroupSgid);
        logger.info(`Added dbid=${databaseId} to the live group`);
      } catch (error) {
        logger.error(`Failed to add dbid=${databaseId} to the live group:`, message(error));
      }
    }

    for (const databaseId of current) {
      if (desired.has(databaseId)) {
        continue;
      }

      try {
        await this.#teamspeak.removeClientFromGroup(databaseId, this.#liveGroupSgid);
        logger.info(`Removed dbid=${databaseId} from the live group`);
      } catch (error) {
        logger.error(`Failed to remove dbid=${databaseId} from the live group:`, message(error));
      }
    }
  }

  async #reconcileStreamGroups(
    existing: ServerGroupRef[],
    desired: Map<string, { streamKey: string; databaseId: string }>,
  ): Promise<void> {
    const existingNames = new Set<string>();
    const stale: ServerGroupRef[] = [];
    for (const group of existing) {
      existingNames.add(group.name);

      if (!desired.has(group.name)) {
        stale.push(group);
      }
    }
    await this.#deleteGroups(stale);

    for (const [name, { streamKey, databaseId }] of desired) {
      if (existingNames.has(name)) {
        continue;
      }

      try {
        await this.#teamspeak.createGroupAndAssign(name, databaseId);
      } catch (error) {
        logger.error(`Failed to create/assign stream group for "${streamKey}":`, message(error));
      }
    }
  }

  async #removeMembers(databaseIds: Set<string>): Promise<void> {
    for (const databaseId of databaseIds) {
      try {
        await this.#teamspeak.removeClientFromGroup(databaseId, this.#liveGroupSgid);
      } catch (error) {
        logger.error(`Failed to remove dbid=${databaseId} from the live group:`, message(error));
      }
    }
  }

  async #deleteGroups(groups: ServerGroupRef[]): Promise<void> {
    for (const group of groups) {
      try {
        await this.#teamspeak.deleteGroup(group);
      } catch (error) {
        logger.error(`Failed to delete group "${group.name}":`, message(error));
      }
    }
  }
}

function message(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
