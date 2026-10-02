import type { ExecutorSnapshot } from "runnerq";
import type { ExecutorReport } from "./executor-report.js";

/** How often a worker reports; the data plane counts a worker gone after three missed. */
export const heartbeatIntervalMs = 10_000;
/** The least time between the extra reports that changes trigger. */
export const minReportGapMs = 2_000;

/** A worker's report (PUT /v1/executors/{id}), in a conductor agent's shapes. */
export function reportOf(snap: ExecutorSnapshot): ExecutorReport {
  const { info, state, counters: c } = snap;
  const started = info.startedAt;
  return {
    sdk: info.sdk,
    executor: {
      id: info.id,
      ...(info.hostname && { hostname: info.hostname }),
      queues: [info.queue],
      ...(info.activityTypes.length && { activity_types: info.activityTypes }),
      max_concurrency: info.maxConcurrency,
      ...(started && { started_at: started.toISOString() }),
      ...(Object.keys(info.labels).length && { labels: info.labels }),
    },
    state: {
      id: info.id,
      uptime_ms: started
        ? Math.max(0, snap.at.getTime() - started.getTime())
        : 0,
      max_concurrency: info.maxConcurrency,
      in_flight: state.running.length,
      ...(state.running.length && {
        running: state.running.map((a) => ({
          activity_id: a.id,
          type: a.type,
          attempt: a.attempt,
          started_at: a.startedAt.toISOString(),
        })),
      }),
      claim_lag_ms: Math.round(c.lastClaimLagMs),
      heartbeat_failures: c.heartbeatFailures,
      draining: state.draining,
      counters: {
        claimed: c.claimed,
        succeeded: c.succeeded,
        retried: c.retried,
        failed: c.failed,
        timed_out: c.timedOut,
        dead_lettered: c.deadLettered,
        claims_lost: c.claimsLost,
      },
    },
  };
}
