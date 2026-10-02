// The RunnerQ Cloud storage protocol (v1), as storaged serves it: one POST per storage
// operation, whose arguments and results are runnerq-go's storage types in Go's JSON.
import { RunnerQError } from "runnerq";
import { storageVersion, type ErrorCode } from "./storage-protocol.js";

/** storaged bounds a normal request at 30s and a long poll at 25s; allow for the network. */
const callTimeoutMs = 35_000;
const maxResponseBytes = 16 << 20;

// The wire types are generated from runnerq-spec's protocol/storage (npm run spec:gen).
export type {
  ActivityResult,
  DequeuedActivity,
  FailureKind,
  IdempotencyResult,
  Operations,
  QueuedActivity,
} from "./storage-protocol.js";

/** runnerq-go's idempotency behaviours, by the TypeScript SDK's duplicate policies. */
export const behavior = {
  allowReuse: 0,
  returnExisting: 1,
  allowReuseOnFailure: 2,
  noReuse: 3,
} as const;

/** The TypeScript SDK's error codes for the protocol's. */
const codes: Record<ErrorCode, ConstructorParameters<typeof RunnerQError>[0]> =
  {
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
 * Sends one request and decodes the envelope. An error code wins over the HTTP status;
 * anything unreadable is `unavailable` (the write may have committed). Redirects are
 * never followed: they would forward the key or replay a write.
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
  // Not AbortSignal.timeout/any: they keep each call's signals and timer alive until the deadline.
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
          "RunnerQ-Storage-Version": storageVersion,
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
      throw new RunnerQError(
        codes[e.code as ErrorCode] ?? "configuration",
        message,
      );
    }
    if (
      res.status !== 200 ||
      res.headers.get("RunnerQ-Storage-Version") !== storageVersion
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
  let url: URL | undefined;
  try {
    url = new URL(raw);
  } catch {}
  if (
    !url?.host ||
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    !(
      url.protocol === "https:" ||
      (url.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname))
    )
  )
    throw new RunnerQError(
      "configuration",
      "provide an HTTPS data-plane endpoint (HTTP is allowed on loopback)",
    );
  return url.toString().replace(/\/+$/, "");
}
