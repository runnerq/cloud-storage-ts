import { createHash, randomUUID } from "node:crypto";
import {
  RunnerQError,
  reportExecutor,
  type ExecutorObserver,
  type ExecutorSource,
  type FailureDetails,
  type SerializedValue,
} from "runnerq";
import type {
  Claim,
  Fence,
  Park,
  Retention,
  Storage,
  StoredResult,
  Submission,
} from "runnerq/storage";
import {
  behavior,
  checkEndpoint,
  request,
  type ActivityResult,
  type DequeuedActivity,
  type FailureKind,
  type IdempotencyResult,
  type QueuedActivity,
  type Transport,
} from "./protocol.js";
import { reportOf, heartbeatIntervalMs, minReportGapMs } from "./report.js";

export interface CloudStorageOptions {
  /** A store key (`rqh_...`) from the RunnerQ Cloud console. */
  apiKey: string;
  /** The data plane's address, e.g. "https://data.runnerq.dev". */
  endpoint: string;
  /** The queue this storage serves; created in the store on first use. Default "default". */
  queue?: string;
  /** For tests and proxies. */
  fetch?: typeof fetch;
}

/** Encodings a TypeScript worker reads. */
const encodings = ["json-v1", "superjson-v1"];
/** How long waitForWork long-polls the data plane (it caps a poll at 25s). */
const defaultWaitMs = 20_000;
const nanos = 1_000_000;

/**
 * RunnerQ Cloud's hosted storage for the TypeScript SDK. Every storage call goes to the
 * data plane over HTTPS with the store key; scheduling, lease recovery and retention run
 * there, not in the worker. A worker built on it also reports itself to the Cloud (every
 * 10 seconds and soon after it changes), so Fleet shows it without an agent.
 */
export class CloudStorage implements Storage, ExecutorObserver {
  readonly queue: string;
  private readonly t: Transport;
  private readonly path: string;
  private readonly reporters = new Map<
    string,
    { stop: AbortController; done: Promise<void> }
  >();
  /** The last claim's shape, so waitForWork can long-poll for the next one. */
  private lastClaim?: {
    limit: number;
    types: readonly string[];
    executorId?: string;
  };
  private stash: Claim[] = [];

  constructor(options: CloudStorageOptions) {
    if (!options.apiKey)
      throw new RunnerQError("configuration", "A store key is required");
    this.queue = options.queue ?? "default";
    if (!/^[\p{L}_][\p{L}\p{N}_]{0,47}$/u.test(this.queue))
      throw new RunnerQError(
        "configuration",
        "Queue names are 1 to 48 letters, digits or underscores, starting with a letter or underscore",
      );
    this.t = {
      endpoint: checkEndpoint(options.endpoint),
      apiKey: options.apiKey,
      fetch: options.fetch ?? fetch,
    };
    this.path = `/v1/queues/${encodeURIComponent(this.queue)}/`;
  }

  private call<T>(
    method: string,
    args: unknown,
    signal?: AbortSignal,
    timeoutMs?: number,
  ): Promise<T> {
    return request<T>(
      this.t,
      "POST",
      this.path + method,
      args,
      signal,
      timeoutMs,
    );
  }

  async submit(a: Submission): Promise<string> {
    const o = a.options;
    const activity: QueuedActivity = {
      ID: a.id,
      ActivityType: a.type,
      Payload: a.payload,
      Priority: ["low", "normal", "high", "critical"].indexOf(o.priority) + 1,
      MaxRetries: o.maxAttempts === "unlimited" ? 0 : o.maxAttempts,
      RetryCount: 0,
      TimeoutSeconds: Math.ceil(o.timeoutMs / 1000),
      // The TypeScript SDK backs off from one second, doubling, up to the maximum.
      RetryDelaySeconds: 1,
      MaxRetryDelaySeconds: Math.ceil(o.maxRetryDelayMs / 1000),
      ScheduledAt:
        o.delayMs > 0 ? new Date(Date.now() + o.delayMs).toISOString() : null,
      Metadata: { ...o.metadata },
      IdempotencyKey: a.key
        ? {
            Key: a.key,
            Behavior: behavior[o.idempotency?.onDuplicate ?? "returnExisting"],
          }
        : null,
      CreatedAt: new Date().toISOString(),
      ParentActivityID: a.parentId,
      RootActivityID: a.rootId,
      Depth: a.depth,
      ...(a.serialization !== "json-v1" && { Serialization: a.serialization }),
    };
    try {
      if (a.key) {
        const existing = a.fence
          ? await this.call<IdempotencyResult | null>(
              "EnqueueIdempotentForWorker",
              {
                a: activity,
                ownerID: a.fence.ownerId,
                workerID: a.fence.token,
              },
            )
          : await this.call<IdempotencyResult | null>("EnqueueIdempotent", {
              activity,
            });
        if (!existing) return a.id;
        if (a.parentId && existing.ExistingParentID !== a.parentId)
          // Best effort, as for Postgres: the lineage event only helps the console.
          await this.call("RecordSpawnLinked", {
            childID: existing.ExistingID,
            parentID: a.parentId,
          }).catch(() => {});
        return existing.ExistingID;
      }
      if (a.fence)
        await this.call("EnqueueForWorker", {
          a: activity,
          ownerID: a.fence.ownerId,
          workerID: a.fence.token,
        });
      else await this.call("Enqueue", { activity });
      return a.id;
    } catch (error) {
      // Caller-generated ids reconcile a lost reply: a retry finds the activity it made.
      if (await this.exists(a.id)) return a.id;
      throw error;
    }
  }
  private async exists(id: string): Promise<boolean> {
    try {
      return !!(await this.call("GetActivity", { activityID: id }));
    } catch {
      return false;
    }
  }

  async claim(
    limit: number,
    types: readonly string[],
    _leaseMs: number,
    executorId?: string,
  ): Promise<Claim[]> {
    if (!types.length || limit < 1) return [];
    this.lastClaim = { limit, types: [...types], executorId };
    const stashed = this.takeStash(limit, types);
    if (stashed.length) return stashed;
    return this.dequeue(limit, types, executorId, 0);
  }
  private takeStash(limit: number, types: readonly string[]): Claim[] {
    const taken = this.stash
      .filter((c) => types.includes(c.type))
      .slice(0, limit);
    this.stash = this.stash.filter((c) => !taken.includes(c));
    return taken;
  }
  private async dequeue(
    limit: number,
    types: readonly string[],
    executorId: string | undefined,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<Claim[]> {
    const prefix = `${executorId ?? randomUUID()}:batch:${randomUUID()}`;
    const claims = await this.call<DequeuedActivity[] | null>(
      "DequeueBatchEncoded",
      {
        workerIDPrefix: prefix,
        limit,
        timeout: timeoutMs * nanos,
        activityTypes: types,
        serializations: encodings,
      },
      signal,
    );
    return (claims ?? []).map(toClaim);
  }

  /**
   * Long-polls the data plane for the next claim, so new work starts at once rather than
   * at the next poll. What it claims is handed out by the next `claim()`.
   */
  async waitForWork(
    signal: AbortSignal,
    timeoutMs = defaultWaitMs,
  ): Promise<void> {
    const last = this.lastClaim;
    if (!last || this.stash.length) return;
    try {
      const claimed = await this.dequeue(
        last.limit,
        last.types,
        last.executorId,
        Math.min(timeoutMs, 25_000),
        signal,
      );
      this.stash.push(...claimed);
    } catch (error) {
      if (signal.aborted) return;
      throw error;
    }
  }

  async renew(f: Fence, leaseMs: number): Promise<boolean> {
    return !!(await this.call<boolean>("ExtendLeaseForWorker", {
      activityID: f.ownerId,
      workerID: f.token,
      extendBy: leaseMs * nanos,
    }));
  }

  async complete(f: Fence, value: SerializedValue): Promise<void> {
    await this.call("AckSuccessEncoded", {
      activityID: f.ownerId,
      result: value.data,
      serialization: value.serialization,
      workerID: f.token,
    });
  }

  async fail(
    f: Fence,
    reason: string,
    retryable: boolean,
    failure?: FailureDetails,
  ): Promise<"failed" | "retrying" | "dead_letter"> {
    const kind: FailureKind = {
      Retryable: retryable,
      Reason: reason,
      IsTimeout: failure?.code === "timeout",
      ...(failure && { Details: failure }),
    };
    const dead = await this.call<boolean>("AckFailure", {
      activityID: f.ownerId,
      failure: kind,
      workerID: f.token,
    });
    if (!retryable) return "failed";
    return dead ? "dead_letter" : "retrying";
  }

  async checkpoint(
    f: Fence,
    resultId: string,
    result: StoredResult,
    step: string,
  ): Promise<void> {
    await this.call("StoreCheckpoint", {
      resultID: resultId,
      ownerID: f.ownerId,
      workerID: f.token,
      result: toActivityResult(result),
      step,
    });
  }

  async getResult(id: string): Promise<StoredResult | null> {
    return toStoredResult(
      await this.call<ActivityResult | null>("GetResult", { activityID: id }),
    );
  }

  async waitResult(id: string, signal?: AbortSignal): Promise<StoredResult> {
    for (;;) {
      signal?.throwIfAborted();
      try {
        const result = toStoredResult(
          await this.call<ActivityResult | null>(
            "WaitForResult",
            { activityID: id },
            signal,
          ),
        );
        if (result) return result;
      } catch (error) {
        // The data plane answers "timeout" when its wait (25s) ends: ask again.
        if (!(error instanceof RunnerQError && error.code === "timeout"))
          throw error;
      }
    }
  }

  async registerDependency(f: Fence, producerId: string): Promise<void> {
    await this.call("RegisterDependency", {
      waiterID: f.ownerId,
      resultID: producerId,
      workerID: f.token,
    });
  }

  async park(f: Fence, wait: Park): Promise<void> {
    if (wait.resultId)
      await this.call("YieldForResult", {
        waiterID: f.ownerId,
        resultID: wait.resultId,
        producerID: wait.producerId ?? null,
        wakeAt: wait.wakeAt,
        workerID: f.token,
        kind: wait.kind,
        step: wait.step,
      });
    else
      await this.call("Yield", {
        activityID: f.ownerId,
        wakeAt: wait.wakeAt,
        workerID: f.token,
        kind: wait.kind,
        step: wait.step,
      });
  }

  async signal(
    id: string,
    name: string,
    payload: SerializedValue,
  ): Promise<void> {
    await this.call("SignalActivityEncoded", {
      activityID: id,
      signalID: signalResultId(id, name),
      name,
      payload: payload.data,
      serialization: payload.serialization,
    });
  }

  async lookupKey(key: string): Promise<string> {
    return this.call<string>("LookupIdempotencyActivityID", {
      idempotencyKey: key,
    });
  }

  /** Lease recovery runs in the data plane. */
  async reap(): Promise<number> {
    return 0;
  }
  /** Retention runs in the data plane: set it on the store in the console. */
  async cleanup(_policy: Retention): Promise<number> {
    throw new RunnerQError(
      "configuration",
      "Retention is managed by RunnerQ Cloud: set it on the store in the console, not on the worker",
    );
  }

  /** Stops reporting; a worker's stop already said goodbye. */
  async close(): Promise<void> {
    await Promise.all(
      [...this.reporters.keys()].map((id) => this.executorStopped(id)),
    );
  }

  executorStarted(source: ExecutorSource): void {
    const id = source.snapshot().info.id;
    if (this.reporters.has(id)) return;
    const stop = new AbortController();
    const done = reportExecutor({
      signal: stop.signal,
      source,
      intervalMs: () => heartbeatIntervalMs,
      minGapMs: minReportGapMs,
      send: async () => {
        try {
          await request(
            this.t,
            "PUT",
            `/v1/executors/${encodeURIComponent(id)}`,
            reportOf(source.snapshot()),
            stop.signal,
            10_000,
          );
        } catch (error) {
          if (!stop.signal.aborted)
            console.warn(
              `runnerq-cloud-storage: executor report failed; retrying: ${(error as Error).message}`,
            );
        }
      },
    });
    this.reporters.set(id, { stop, done });
  }

  async executorStopped(id: string): Promise<void> {
    const reporter = this.reporters.get(id);
    this.reporters.delete(id);
    if (reporter) {
      reporter.stop.abort();
      await reporter.done;
    }
    try {
      await request(
        this.t,
        "DELETE",
        `/v1/executors/${encodeURIComponent(id)}`,
        undefined,
        undefined,
        5_000,
      );
    } catch (error) {
      console.warn(
        `runnerq-cloud-storage: executor goodbye failed: ${(error as Error).message}`,
      );
    }
  }
}

function toClaim(d: DequeuedActivity): Claim {
  const a = d.Activity;
  return {
    serialization: (a.Serialization || "json-v1") as Claim["serialization"],
    id: a.ID,
    type: a.ActivityType,
    payload: a.Payload as Claim["payload"],
    token: d.LeaseID,
    retryCount: a.RetryCount,
    timeoutMs: a.TimeoutSeconds * 1000,
    parentId: a.ParentActivityID,
    rootId: a.RootActivityID,
    depth: a.Depth,
    metadata: a.Metadata ?? {},
    leaseDeadlineMs: Date.parse(d.LeaseDeadline),
    dueAt: a.ScheduledAt ?? a.CreatedAt,
  };
}
function toActivityResult(r: StoredResult): ActivityResult {
  return {
    Data: r.data,
    State: r.state === "Ok" ? 0 : 1,
    ...(r.serialization !== "json-v1" && { Serialization: r.serialization }),
  };
}
function toStoredResult(r: ActivityResult | null): StoredResult | null {
  if (!r) return null;
  return {
    state: r.State === 0 ? "Ok" : "Err",
    data: r.Data as StoredResult["data"],
    serialization: (r.Serialization ||
      "json-v1") as StoredResult["serialization"],
  };
}

/**
 * Where a signal's payload is stored: UUIDv5 in the activity's namespace of
 * "signal:<name>", as both SDKs derive it.
 */
export function signalResultId(activityId: string, name: string): string {
  const digest = createHash("sha1")
    .update(Buffer.from(activityId.replaceAll("-", ""), "hex"))
    .update(`signal:${name}`, "utf8")
    .digest();
  digest[6] = (digest[6]! & 0x0f) | 0x50;
  digest[8] = (digest[8]! & 0x3f) | 0x80;
  const h = digest.subarray(0, 16).toString("hex");
  return `${h.slice(0, 8)}-${h.slice(8, 12)}-${h.slice(12, 16)}-${h.slice(16, 20)}-${h.slice(20)}`;
}
