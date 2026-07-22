import ky, { type KyInstance } from "ky";
import { z } from "zod";
import type { Config } from "./config.ts";
import type { Logger } from "./logger.ts";

/**
 * Lenient schema for a Broadcast Box `StreamSessionState` (the admin status
 * endpoint returns a JSON array of these). Only the fields we rely on are
 * described; any others are preserved by `.loose()`.
 */
const streamSessionSchema = z
  .object({
    streamKey: z.string().optional(),
    streamStart: z.union([z.number(), z.string()]).optional(),
    videoTracks: z.array(z.unknown()).optional(),
    audioTracks: z.array(z.unknown()).optional(),
  })
  .loose();

const statusSchema = z.array(streamSessionSchema);

type StreamSession = z.infer<typeof streamSessionSchema>;

function hasEntries(value: readonly unknown[] | undefined): boolean {
  return value !== undefined && value.length > 0;
}

function isTruthyTimestamp(value: number | string | undefined): boolean {
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
function isLive(state: StreamSession): state is StreamSession & { streamKey: string } {
  return (
    typeof state.streamKey === "string" &&
    state.streamKey.trim() !== "" &&
    (isTruthyTimestamp(state.streamStart) ||
      hasEntries(state.videoTracks) ||
      hasEntries(state.audioTracks))
  );
}

export class BroadcastBoxClient {
  readonly #logger: Logger;
  readonly #api: KyInstance;

  constructor(config: Config, logger: Logger) {
    this.#logger = logger;
    this.#api = ky.create({
      prefix: config.broadcastBox.apiUrl,
      headers: {
        Authorization: config.broadcastBox.authorization,
        Accept: "application/json",
      },
      retry: { limit: 2 },
      timeout: 10_000,
    });
  }

  /**
   * Fetches the currently live stream keys from `/api/admin/status`.
   *
   * The admin endpoint is used because Broadcast Box runs with
   * `DISABLE_STATUS=true`, which turns off the public `/api/status` route.
   *
   * Throws `ky.HTTPError` on a non-2xx response and `SchemaValidationError`
   * when the payload does not match the expected shape.
   */
  async fetchLiveStreamKeys(signal?: AbortSignal): Promise<Set<string>> {
    const sessions = await this.#api.get("api/admin/status", { signal }).json(statusSchema);

    const live = new Set<string>();

    for (const session of sessions) {
      if (isLive(session)) {
        live.add(session.streamKey);
      }
    }

    this.#logger.debug(`Broadcast Box reports ${live.size} live stream(s)`);

    return live;
  }
}
