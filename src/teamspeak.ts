import { QueryProtocol, TeamSpeak } from "ts3-nodejs-library";
import type { Config } from "./config.ts";
import type { Logger } from "./logger.ts";

/** A regular (non-template) server group. */
const SERVER_GROUP_TYPE_REGULAR = 1;

export interface TeamSpeakClientInfo {
  nickname: string;
  databaseId: string;
}

export interface TemporaryGroup {
  sgid: string;
  name: string;
}

/**
 * Thin wrapper around a TeamSpeak ServerQuery connection exposing only the
 * operations the watcher needs, plus transparent reconnection.
 */
export class TeamSpeakManager {
  readonly #config: Config;
  readonly #logger: Logger;
  #query: TeamSpeak;

  private constructor(config: Config, logger: Logger, query: TeamSpeak) {
    this.#config = config;
    this.#logger = logger;
    this.#query = query;
  }

  static async connect(config: Config, logger: Logger): Promise<TeamSpeakManager> {
    const query = await TeamSpeak.connect({
      host: config.teamspeak.host,
      protocol: QueryProtocol.RAW,
      queryport: config.teamspeak.queryPort,
      serverport: config.teamspeak.serverPort,
      username: config.teamspeak.username,
      password: config.teamspeak.password,
      nickname: config.teamspeak.nickname,
    });

    const manager = new TeamSpeakManager(config, logger, query);
    manager.#attachHandlers();
    logger.info(
      `Connected to TeamSpeak ServerQuery at ${config.teamspeak.host}:${config.teamspeak.queryPort}`,
    );

    return manager;
  }

  #attachHandlers(): void {
    this.#query.on("error", (error) => {
      this.#logger.error("TeamSpeak connection error:", error.message);
    });
    this.#query.on("close", (error) => {
      this.#logger.warn(
        `TeamSpeak connection closed${error ? `: ${error.message}` : ""}. Reconnecting…`,
      );
      // Reconnect forever; the library restores the selected virtual server
      // and re-registers context on success.
      this.#query.reconnect(-1, 2000).then(
        () => this.#logger.info("Reconnected to TeamSpeak ServerQuery"),
        (reason: unknown) => this.#logger.error("TeamSpeak reconnect failed:", reason),
      );
    });
  }

  /** Lists regular (non-query) connected clients. */
  async listClients(): Promise<TeamSpeakClientInfo[]> {
    const clients = await this.#query.clientList({ clientType: 0 });

    return clients.map((client) => ({
      nickname: client.nickname,
      databaseId: client.databaseId,
    }));
  }

  /** Lists server groups whose name starts with the configured prefix. */
  async listTemporaryGroups(): Promise<TemporaryGroup[]> {
    const groups = await this.#query.serverGroupList();

    return groups
      .filter((group) => group.name.startsWith(this.#config.groupPrefix))
      .map((group) => ({ sgid: group.sgid, name: group.name }));
  }

  /** Creates a regular server group and assigns the given client to it. */
  async createGroupAndAssign(name: string, databaseId: string): Promise<void> {
    const group = await this.#query.serverGroupCreate(name, SERVER_GROUP_TYPE_REGULAR);
    await this.#query.serverGroupAddClient(databaseId, group.sgid);
    this.#logger.info(`Created group "${name}" and assigned client dbid=${databaseId}`);
  }

  /** Deletes a server group (force-removing any members). */
  async deleteGroup(group: TemporaryGroup): Promise<void> {
    await this.#query.serverGroupDel(group.sgid, true);
    this.#logger.info(`Deleted group "${group.name}"`);
  }

  async disconnect(): Promise<void> {
    await this.#query.quit();
  }
}
