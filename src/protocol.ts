// The RunnerQ Cloud storage protocol (v1), as storaged serves it: one POST per storage
// operation, whose arguments and results are runnerq-go's storage types in Go's JSON.
import { RunnerQError } from "runnerq";

const protocolVersion = "1";
/** storaged bounds a normal request at 30s and a long poll at 25s; allow for the network. */
const callTimeoutMs = 35_000;
const maxResponseBytes = 16 << 20;

/** runnerq-go's storage.QueuedActivity. */
export interface QueuedActivity {
  ID: string;
  ActivityType: string;
  Payload: unknown;
  Priority: number;
  MaxRetries: number;
  RetryCount: number;
  TimeoutSeconds: number;
  RetryDelaySeconds: number;
  MaxRetryDelaySeconds: number;
  ScheduledAt: string | null;
  Metadata: Record<string, string> | null;
  IdempotencyKey: { Key: string; Behavior: number } | null;
  CreatedAt: string;
  ParentActivityID: string | null;
  RootActivityID: string;
  Depth: number;
  /** Absent for plain JSON. */
  Serialization?: string;
}
export interface DequeuedActivity {
  Activity: QueuedActivity;
  LeaseID: string;
  Attempt: number;
  LeaseDeadline: string;
}
export interface ActivityResult {
  Data: unknown;
  /** 0 ok, 1 error. */
  State: number;
  Serialization?: string;
}
export interface IdempotencyResult {
  ExistingID: string;
  ExistingParentID: string | null;
}
export interface FailureKind {
  Retryable: boolean;
  Reason: string;
  IsTimeout: boolean;
  Details?: unknown;
}

/** runnerq-go's idempotency behaviours, by the TypeScript SDK's duplicate policies. */
export const behavior = {
  allowReuse: 0,
  returnExisting: 1,
  allowReuseOnFailure: 2,
  noReuse: 3,
} as const;

/** The TypeScript SDK's error codes for the protocol's. */
const codes: Record<string, ConstructorParameters<typeof RunnerQError>[0]> = {
  unavailable: "unavailable",
  conflict: "conflict",
  not_found: "not_found",
  internal: "internal",
  serialization: "serialization",
  configuration: "configuration",
  timeout: "timeout",
  duplicate_activity: "duplicate",
  idempotency_conflict: "idempotency_conflict",
  claim_lost: "claim_lost",
  checkpoint_conflict: "checkpoint_conflict",
  invalid_argument: "configuration",
  unsupported: "configuration",
};

export interface Transport {
  endpoint: string;
  apiKey: string;
  fetch: typeof fetch;
}

/**
 * Sends one request and decodes storaged's envelope. A symbolic error code wins over the
 * HTTP status; anything unreadable is `unavailable` (the outcome may be unknown, and the
 * worker retries idempotent calls). Redirects are never followed: they would forward the
 * key or replay a write.
 */
export async function request<T>(
  t: Transport,
  method: "POST" | "PUT" | "DELETE",
  path: string,
  body: unknown,
  signal?: AbortSignal,
  timeoutMs = callTimeoutMs,
): Promise<T> {
  signal?.throwIfAborted();
  // A cleared timer rather than AbortSignal.timeout/any, which keep every call's signals
  // and timer alive until the deadline passes.
  const abort = new AbortController();
  const timer = setTimeout(
    () =>
      abort.abort(
        new DOMException(
          "The operation was aborted due to timeout",
          "TimeoutError",
        ),
      ),
    timeoutMs,
  ).unref();
  const cancel = () => abort.abort(signal!.reason);
  signal?.addEventListener("abort", cancel);
  try {
    let res: Response;
    try {
      res = await t.fetch(t.endpoint + path, {
        method,
        redirect: "manual",
        signal: abort.signal,
        headers: {
          Authorization: `Bearer ${t.apiKey}`,
          "Content-Type": "application/json",
          "RunnerQ-Storage-Version": protocolVersion,
        },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
    } catch (cause) {
      if (signal?.aborted) throw signal.reason ?? cause;
      throw new RunnerQError(
        abort.signal.aborted ? "timeout" : "unavailable",
        "storage transport failed; outcome may be unknown",
        { cause },
      );
    }
    let text: string;
    try {
      text = await res.text();
    } catch (cause) {
      if (signal?.aborted) throw signal.reason ?? cause;
      throw new RunnerQError(
        "unavailable",
        "incomplete storage response; outcome may be unknown",
        { cause },
      );
    }
    if (text.length > maxResponseBytes)
      throw new RunnerQError("unavailable", "storage response too large");
    let envelope: {
      result?: unknown;
      error?: { code: string; message: string; field?: string };
    };
    try {
      envelope = text ? JSON.parse(text) : {};
    } catch (cause) {
      throw new RunnerQError(
        "unavailable",
        `invalid storage response (${res.status}); outcome may be unknown`,
        { cause },
      );
    }
    if (envelope.error) {
      const e = envelope.error;
      const message = e.field ? `${e.message} (${e.field})` : e.message;
      throw new RunnerQError(codes[e.code] ?? "configuration", message);
    }
    if (
      res.status !== 200 ||
      res.headers.get("RunnerQ-Storage-Version") !== protocolVersion
    )
      throw new RunnerQError(
        "unavailable",
        `unexpected storage response (${res.status})`,
      );
    return envelope.result as T;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", cancel);
  }
}

/** Checks the data-plane endpoint: HTTPS, or HTTP on loopback; no credentials in it. */
export function checkEndpoint(raw: string): string {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    throw invalidEndpoint();
  }
  const loopback = ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname);
  if (
    !url.host ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !(url.protocol === "https:" || (url.protocol === "http:" && loopback))
  )
    throw invalidEndpoint();
  return url.toString().replace(/\/+$/, "");
}
function invalidEndpoint(): RunnerQError {
  return new RunnerQError(
    "configuration",
    "provide an HTTPS data-plane endpoint (HTTP is allowed on loopback)",
  );
}
