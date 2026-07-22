import { BroadcastBoxClient } from "./broadcast-box.ts";
import { config } from "./config.ts";
import { logger } from "./logger.ts";
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
  logger.info("Starting bbox-ts-live");
  logger.debug(
    `Broadcast Box: ${config.broadcastBox.apiUrl} · public host: ${config.publicStreamHost} · ` +
      `poll: ${config.pollIntervalMs}ms · live group: "${config.liveGroupName}" · ` +
      `stream prefix: "${config.streamGroupPrefix}"`,
  );

  const broadcastBox = new BroadcastBoxClient(config.broadcastBox);
  const teamspeak = await TeamSpeakManager.connect(config.teamspeak);
  const liveGroupSgid = await teamspeak.ensureLiveGroup(config.liveGroupName);
  const watcher = new Watcher(broadcastBox, teamspeak, liveGroupSgid, config);

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

  // Best-effort cleanup: clear the live group and delete per-user stream groups.
  try {
    await watcher.cleanup();
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
