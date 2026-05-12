import type { RunId, Trigger } from "@nagi-js/core";

/**
 * Minimal shape the trigger needs from a LISTEN-capable Postgres client.
 * Compatible with `pg.Client` (Node) and a thin wrapper over `pg.Pool` /
 * `@neondatabase/serverless`'s `Client`. Kysely does not expose a long-lived
 * connection for LISTEN, so this is a separate driver handle by design.
 */
export interface ListenClient {
  query(text: string): Promise<unknown>;
  on(event: "notification", handler: (msg: NotificationMessage) => void): void;
  removeListener?(
    event: "notification",
    handler: (msg: NotificationMessage) => void,
  ): void;
  off?(event: "notification", handler: (msg: NotificationMessage) => void): void;
}

export interface NotificationMessage {
  readonly channel: string;
  readonly payload?: string | undefined;
}

export interface PostgresTriggerOpts {
  /**
   * Long-lived LISTEN client. The trigger calls `LISTEN <channel>` on it once
   * and never closes it — lifetime is the user's. Reuse one client per
   * worker process; do not share with query workloads (pg's `Pool` rotates
   * connections, which would drop `LISTEN` state).
   */
  readonly listen: ListenClient;
  /** NOTIFY channel name. Default `nagi`. Must match the Store's `notifyChannel`. */
  readonly channel?: string;
}

/**
 * Wake the scheduler when the Store writes facts. Pair with
 * `postgresStore({ notifyChannel })` so every `appendFact` /
 * `completeStep` / `failStep` emits `pg_notify(channel, runId)`.
 *
 * Subscribers receive only the `runId`. The scheduler is responsible for
 * loading the run's state and deciding what to advance.
 */
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

      // Lazy: only start LISTEN when the first subscriber attaches.
      if (!started) {
        started = true;
        opts.listen.on("notification", onNotification);
        // Fire-and-forget — the user controls the LISTEN client lifecycle.
        // Errors here surface as `query()` promise rejections; we propagate
        // to the next microtask so they don't get silently swallowed.
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

/**
 * Same identifier rule as the schema name — interpolated raw into
 * `LISTEN <channel>` since pg has no parameter binding for identifiers.
 */
function assertValidChannel(channel: string): void {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(channel)) {
    throw new Error(
      `@nagi-js/postgres: invalid NOTIFY channel "${channel}". Must match /^[A-Za-z_][A-Za-z0-9_]*$/.`,
    );
  }
}
