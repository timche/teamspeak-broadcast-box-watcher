import { BroadcastBoxClient } from "./broadcast-box.ts";
import { loadConfig } from "./config.ts";
import { createLogger } from "./logger.ts";
import { TeamSpeakManager } from "./teamspeak.ts";
import { Watcher } from "./watcher.ts";

function delay(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timer);
        resolve();
      },
      { once: true },
    );
  });
}

async function main(): Promise<void> {
  const config = loadConfig();
  const logger = createLogger(config.logLevel);

  logger.info("Starting teamspeak-broadcast-box-watcher");
  logger.debug(
    `Broadcast Box: ${config.broadcastBox.apiUrl} · public host: ${config.publicStreamHost} · ` +
      `poll: ${config.pollIntervalMs}ms · prefix: "${config.groupPrefix}"`,
  );

  const broadcastBox = new BroadcastBoxClient(config, logger);
  const teamspeak = await TeamSpeakManager.connect(config, logger);
  const watcher = new Watcher(config, logger, broadcastBox, teamspeak);

  const abort = new AbortController();
  let shuttingDown = false;

  const shutdown = (reason: string): void => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    logger.info(`Received ${reason}, shutting down…`);
    abort.abort();
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));

  while (!abort.signal.aborted) {
    try {
      await watcher.reconcile(abort.signal);
    } catch (error) {
      if (!abort.signal.aborted) {
        logger.error(
          "Reconcile cycle failed:",
          error instanceof Error ? error.message : String(error),
        );
      }
    }

    await delay(config.pollIntervalMs, abort.signal);
  }

  // Best-effort cleanup: remove any temporary groups we may have created.
  try {
    const leftovers = await teamspeak.listTemporaryGroups();
    for (const group of leftovers) {
      await teamspeak.deleteGroup(group);
    }
  } catch (error) {
    logger.error(
      "Cleanup during shutdown failed:",
      error instanceof Error ? error.message : String(error),
    );
  }

  await teamspeak.disconnect().catch(() => undefined);
  logger.info("Shutdown complete");
}

main().catch((error: unknown) => {
  console.error(error instanceof Error ? (error.stack ?? error.message) : String(error));
  process.exit(1);
});
