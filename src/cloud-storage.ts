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
  type Operations,
  type QueuedActivity,
  type Transport,
} from "./protocol.js";
import type { Priority } from "./storage-protocol.js";
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
const priorities = ["low", "normal", "high", "critical"];

/**
 * RunnerQ Cloud's hosted storage: every call goes to the data plane, which also runs
 * scheduling, lease recovery and retention. Workers on it report themselves to Fleet.
 */
export class CloudStorage implements Storage, ExecutorObserver {
  readonly queue: string;
  private readonly t: Transport;
  private readonly path: string;
  private readonly reporters = new Map<
    string,
    { stop: AbortController; done: Promise<void>; source: ExecutorSource }
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

  /** One storage operation; the schema types its arguments and result. */
  private call<K extends keyof Operations & string>(
    method: K,
    args: Operations[K]["args"],
    signal?: AbortSignal,
    timeoutMs?: number,
  ): Promise<Operations[K]["result"]> {
    return request<Operations[K]["result"]>(
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
      Priority: (priorities.indexOf(o.priority) + 1) as Priority,
      MaxRetries: o.maxAttempts === "unlimited" ? 0 : o.maxAttempts,
      RetryCount: 0,
      TimeoutSeconds: Math.ceil(o.timeoutMs / 1000),
      // The TypeScript SDK backs off from one second, doubling, up to the maximum.
      RetryDelaySeconds: 1,
      MaxRetryDelaySeconds: Math.ceil(o.maxRetryDelayMs / 1000),
      ScheduledAt:
        o.delayMs > 0 ? new Date(Date.now() + o.delayMs).toISOString() : null,
      Metadata: o.metadata,
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
          ? await this.call("EnqueueIdempotentForWorker", {
              a: activity,
              ownerID: a.fence.ownerId,
              workerID: a.fence.token,
            })
          : await this.call("EnqueueIdempotent", {
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
      // The write may have committed and lost its reply: the caller-generated id finds it.
      // Any failure to look counts as not stored, so submit throws its own error.
      const stored = await this.call("GetActivity", { activityID: a.id }).catch(
        () => null,
      );
      if (stored) return a.id;
      throw error;
    }
  }

  async claim(
    limit: number,
    types: readonly string[],
    _leaseMs: number,
    executorId?: string,
  ): Promise<Claim[]> {
    if (!types.length || limit < 1) return [];
    this.lastClaim = { limit, types, executorId };
    if (this.stash.length) {
      const taken: Claim[] = [];
      const kept: Claim[] = [];
      for (const c of this.stash)
        (taken.length < limit && types.includes(c.type) ? taken : kept).push(c);
      this.stash = kept;
      if (taken.length) return taken;
    }
    return this.dequeue(limit, types, executorId, 0);
  }
  private async dequeue(
    limit: number,
    types: readonly string[],
    executorId: string | undefined,
    timeoutMs: number,
    signal?: AbortSignal,
  ): Promise<Claim[]> {
    const prefix = `${executorId ?? randomUUID()}:batch:${randomUUID()}`;
    const claims = await this.call(
      "DequeueBatchEncoded",
      {
        workerIDPrefix: prefix,
        limit,
        timeout: timeoutMs * nanos,
        activityTypes: [...types],
        serializations: encodings,
      },
      signal,
    );
    return (claims ?? []).map(toClaim);
  }

  /** Long-polls for the next claim, so work starts at once; the next `claim()` takes it. */
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
    return !!(await this.call("ExtendLeaseForWorker", {
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
    const dead = await this.call("AckFailure", {
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
    return toStoredResult(await this.call("GetResult", { activityID: id }));
  }

  async waitResult(id: string, signal?: AbortSignal): Promise<StoredResult> {
    for (;;) {
      signal?.throwIfAborted();
      try {
        const result = toStoredResult(
          await this.call("WaitForResult", { activityID: id }, signal),
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
    return this.call("LookupIdempotencyActivityID", {
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
      send: () =>
        this.executorCall(
          "PUT",
          id,
          reportOf(source.snapshot()),
          stop.signal,
        ).catch((error) => {
          if (!stop.signal.aborted)
            warn("executor report failed; retrying", error);
        }),
    });
    this.reporters.set(id, { stop, done, source });
  }

  /**
   * Stops reporting, sends a final report (the loop's may be a heartbeat old, and Fleet
   * keeps what a stopped worker last said), then says goodbye.
   */
  async executorStopped(id: string): Promise<void> {
    const reporter = this.reporters.get(id);
    this.reporters.delete(id);
    if (reporter) {
      reporter.stop.abort();
      await reporter.done;
      await this.executorCall(
        "PUT",
        id,
        reportOf(reporter.source.snapshot()),
      ).catch((error) => warn("final executor report failed", error));
    }
    await this.executorCall("DELETE", id).catch((error) =>
      warn("executor goodbye failed", error),
    );
  }

  private executorCall(
    method: "PUT" | "DELETE",
    id: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<void> {
    return request(
      this.t,
      method,
      `/v1/executors/${encodeURIComponent(id)}`,
      body,
      signal,
      5_000,
    );
  }
}

function warn(what: string, error: unknown): void {
  console.warn(`runnerq-cloud-storage: ${what}: ${(error as Error).message}`);
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
    parentId: a.ParentActivityID ?? null,
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
