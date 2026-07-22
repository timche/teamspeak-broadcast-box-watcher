import { parseLogLevel, type LogLevel } from "./logger.ts";

export interface Config {
  broadcastBox: {
    apiUrl: string;
    /** Base64-encoded bearer token derived from the cleartext env value. */
    authorization: string;
  };
  /** Public host shown in the temporary group name, e.g. `stream.example.com`. */
  publicStreamHost: string;
  teamspeak: {
    host: string;
    queryPort: number;
    serverPort: number;
    username: string;
    password: string;
    nickname: string;
  };
  pollIntervalMs: number;
  groupPrefix: string;
  logLevel: LogLevel;
}

class ConfigError extends Error {}

function required(env: Record<string, string | undefined>, key: string): string {
  const value = env[key];

  if (value === undefined || value.trim() === "") {
    throw new ConfigError(`Missing required environment variable: ${key}`);
  }

  return value;
}

function optional(env: Record<string, string | undefined>, key: string, fallback: string): string {
  const value = env[key];

  return value === undefined || value.trim() === "" ? fallback : value;
}

function integer(env: Record<string, string | undefined>, key: string, fallback: number): number {
  const value = env[key];

  if (value === undefined || value.trim() === "") {
    return fallback;
  }

  const parsed = Number(value);

  if (!Number.isInteger(parsed) || parsed <= 0) {
    throw new ConfigError(`Environment variable ${key} must be a positive integer, got: ${value}`);
  }

  return parsed;
}

/**
 * Parses and validates the process environment into a {@link Config}.
 *
 * Throws a descriptive error (never leaking secret values) when a required
 * variable is missing or malformed.
 */
export function loadConfig(env: Record<string, string | undefined> = process.env): Config {
  const token = required(env, "BROADCAST_BOX_ADMIN_TOKEN");

  return {
    broadcastBox: {
      apiUrl: required(env, "BROADCAST_BOX_API_URL").replace(/\/+$/, ""),
      // The env var holds the token in cleartext; Broadcast Box expects it
      // base64-encoded in the Authorization header.
      authorization: `Bearer ${Buffer.from(token, "utf8").toString("base64")}`,
    },
    publicStreamHost: required(env, "PUBLIC_STREAM_HOST")
      .replace(/^https?:\/\//, "")
      .replace(/\/+$/, ""),
    teamspeak: {
      host: required(env, "TEAMSPEAK_HOST"),
      queryPort: integer(env, "TEAMSPEAK_QUERY_PORT", 10011),
      serverPort: integer(env, "TEAMSPEAK_SERVER_PORT", 9987),
      username: optional(env, "TEAMSPEAK_QUERY_USERNAME", "serveradmin"),
      password: required(env, "TEAMSPEAK_QUERY_PASSWORD"),
      nickname: optional(env, "TEAMSPEAK_QUERY_NICKNAME", "bbox-ts-live"),
    },
    pollIntervalMs: integer(env, "POLL_INTERVAL_MS", 10_000),
    groupPrefix: optional(env, "GROUP_PREFIX", "🔴"),
    logLevel: parseLogLevel(env["LOG_LEVEL"]),
  };
}
