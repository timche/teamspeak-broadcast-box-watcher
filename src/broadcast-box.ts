import type { Config } from "./config.ts";
import type { Logger } from "./logger.ts";

/**
 * Subset of a Broadcast Box `StreamSessionState` (returned as a JSON array by
 * `GET /api/admin/status`) that we rely on. Extra fields are ignored.
 */
interface StreamSessionState {
  streamKey?: unknown;
  streamStart?: unknown;
  videoTracks?: unknown;
  audioTracks?: unknown;
}

function hasEntries(value: unknown): boolean {
  return Array.isArray(value) && value.length > 0;
}

function isTruthyTimestamp(value: unknown): boolean {
  if (typeof value === "number") {
    return value > 0;
  }

  if (typeof value === "string") {
    return value.trim() !== "" && value !== "0";
  }

  return false;
}

/**
 * A stream counts as live when it exposes a stream key and shows an active
 * publisher — signalled by a start timestamp or by any received media track.
 */
function isLive(state: StreamSessionState): state is StreamSessionState & { streamKey: string } {
  return (
    typeof state.streamKey === "string" &&
    state.streamKey.trim() !== "" &&
    (isTruthyTimestamp(state.streamStart) ||
      hasEntries(state.videoTracks) ||
      hasEntries(state.audioTracks))
  );
}

export class BroadcastBoxClient {
  readonly #config: Config;
  readonly #logger: Logger;

  constructor(config: Config, logger: Logger) {
    this.#config = config;
    this.#logger = logger;
  }

  /**
   * Fetches the currently live stream keys from `/api/admin/status`.
   *
   * The admin endpoint is used because Broadcast Box runs with
   * `DISABLE_STATUS=true`, which turns off the public `/api/status` route.
   */
  async fetchLiveStreamKeys(signal?: AbortSignal): Promise<Set<string>> {
    const url = `${this.#config.broadcastBox.apiUrl}/api/admin/status`;

    const response = await fetch(url, {
      headers: {
        Authorization: this.#config.broadcastBox.authorization,
        Accept: "application/json",
      },
      signal: signal ?? null,
    });

    if (!response.ok) {
      throw new Error(`Broadcast Box responded with ${response.status} ${response.statusText}`);
    }

    const body: unknown = await response.json();

    if (!Array.isArray(body)) {
      throw new Error("Unexpected Broadcast Box response: expected a JSON array");
    }

    const live = new Set<string>();

    for (const entry of body as StreamSessionState[]) {
      if (isLive(entry)) {
        live.add(entry.streamKey);
      }
    }

    this.#logger.debug(`Broadcast Box reports ${live.size} live stream(s)`);

    return live;
  }
}
