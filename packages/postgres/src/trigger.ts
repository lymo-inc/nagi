import type { RunId, Trigger } from "@nagi-js/core";

export interface ListenClient {
  query(text: string): Promise<unknown>;
  on(event: "notification", handler: (msg: NotificationMessage) => void): void;
  removeListener?(
    event: "notification",
    handler: (msg: NotificationMessage) => void,
  ): void;
  off?(
    event: "notification",
    handler: (msg: NotificationMessage) => void,
  ): void;
}

export interface NotificationMessage {
  readonly channel: string;
  readonly payload?: string | undefined;
}

export interface PostgresTriggerOpts {
  readonly listen: ListenClient;
  readonly channel?: string;
}

export function postgresTrigger(opts: PostgresTriggerOpts): Trigger {
  const channel = opts.channel ?? "nagi";
  assertValidChannel(channel);

  const handlers = new Set<(runId: RunId) => void>();
  let started = false;

  const onNotification = (msg: NotificationMessage): void => {
    if (msg.channel !== channel) return;
    const runId = (msg.payload ?? "") as RunId;
    if (!runId) return;
    for (const h of handlers) h(runId);
  };

  return {
    subscribe(handler: (runId: RunId) => void): () => void {
      handlers.add(handler);

      if (!started) {
        started = true;
        opts.listen.on("notification", onNotification);
        void opts.listen.query(`LISTEN ${channel}`).catch((err) => {
          queueMicrotask(() => {
            throw err;
          });
        });
      }

      return () => {
        handlers.delete(handler);
      };
    },
  };
}

function assertValidChannel(channel: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(channel)) {
    throw new Error(
      `@nagi-js/postgres: invalid NOTIFY channel "${channel}". Must match /^[A-Za-z_][A-Za-z0-9_]*$/.`,
    );
  }
}
