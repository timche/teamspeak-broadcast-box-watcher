const LEVELS = ["debug", "info", "warn", "error"] as const;

export type LogLevel = (typeof LEVELS)[number];

function isLogLevel(value: string): value is LogLevel {
  return (LEVELS as readonly string[]).includes(value);
}

export function parseLogLevel(value: string | undefined, fallback: LogLevel = "info"): LogLevel {
  if (value === undefined) {
    return fallback;
  }

  const normalized = value.toLowerCase();

  return isLogLevel(normalized) ? normalized : fallback;
}

export interface Logger {
  debug(message: string, ...args: unknown[]): void;
  info(message: string, ...args: unknown[]): void;
  warn(message: string, ...args: unknown[]): void;
  error(message: string, ...args: unknown[]): void;
}

export function createLogger(level: LogLevel): Logger {
  const threshold = LEVELS.indexOf(level);

  function log(target: LogLevel, message: string, args: unknown[]): void {
    if (LEVELS.indexOf(target) < threshold) {
      return;
    }

    const line = `${new Date().toISOString()} ${target.toUpperCase().padEnd(5)} ${message}`;

    if (target === "error") {
      console.error(line, ...args);
    } else if (target === "warn") {
      console.warn(line, ...args);
    } else {
      console.log(line, ...args);
    }
  }

  return {
    debug: (message, ...args) => log("debug", message, args),
    info: (message, ...args) => log("info", message, args),
    warn: (message, ...args) => log("warn", message, args),
    error: (message, ...args) => log("error", message, args),
  };
}
