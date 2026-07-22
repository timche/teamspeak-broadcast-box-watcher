import ky, { type KyInstance } from "ky";
import { z } from "zod";
import { logger } from "./logger.ts";

/**
 * Lenient schema for a Broadcast Box `StreamSessionState` (the admin status
 * endpoint returns a JSON array of these). Only the fields we rely on are
 * described; any others are preserved by `.loose()`.
 */
const streamSessionSchema = z
  .object({
    streamKey: z.string().optional(),
    videoTracks: z.array(z.unknown()).optional(),
    audioTracks: z.array(z.unknown()).optional(),
  })
  .loose();

/**
 * The admin status endpoint returns a JSON array of sessions, or `null` when
 * no streams exist at all. Normalise the `null` case to an empty array.
 */
const statusSchema = z
  .array(streamSessionSchema)
  .nullable()
  .transform((sessions) => sessions ?? []);

type StreamSession = z.infer<typeof streamSessionSchema>;

function hasEntries(value: readonly unknown[] | undefined): boolean {
  return value !== undefined && value.length > 0;
}

/**
 * A stream counts as live when it exposes a stream key and is actively
 * publishing — signalled by any received media track. `streamStart` is set as
 * soon as a stream key is provisioned (even while offline), so it can't be
 * used to decide liveness.
 */
function isLive(state: StreamSession): state is StreamSession & { streamKey: string } {
  return (
    typeof state.streamKey === "string" &&
    state.streamKey.trim() !== "" &&
    (hasEntries(state.videoTracks) || hasEntries(state.audioTracks))
  );
}

export class BroadcastBoxClient {
  readonly #api: KyInstance;

  constructor(options: { apiUrl: string; authorization: string }) {
    this.#api = ky.create({
      prefix: options.apiUrl,
      headers: {
        Authorization: options.authorization,
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

    logger.debug(`Broadcast Box reports ${live.size} live stream(s)`);

    return live;
  }
}
