import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { randomUUID } from "node:crypto";
import { RunnerQError } from "runnerq";
import { CloudStorage, signalResultId } from "../dist/index.js";

// fakeData is storaged: it records each request and answers from `answer`.
async function fakeData(t, answer = () => ({ result: null })) {
  const calls = [];
  const server = http.createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) body += chunk;
    const call = {
      method: req.method,
      path: req.url,
      headers: req.headers,
      body: body ? JSON.parse(body) : undefined,
    };
    calls.push(call);
    const out = await answer(call);
    if (out.redirect) {
      res.writeHead(307, { Location: out.redirect });
      return res.end();
    }
    res.writeHead(out.status ?? (out.error ? 409 : 200), {
      "Content-Type": "application/json",
      "RunnerQ-Storage-Version": out.version ?? "1",
    });
    res.end(
      JSON.stringify(
        out.error ? { error: out.error } : { result: out.result ?? null },
      ),
    );
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  t.after(() => new Promise((r) => server.close(r)));
  return { url: `http://127.0.0.1:${server.address().port}`, calls };
}
const op = (call) => call.path.split("/").at(-1);
function submission(extra = {}) {
  const id = randomUUID();
  return {
    serialization: "json-v1",
    id,
    type: "charge",
    payload: { amount: 10 },
    options: {
      priority: "high",
      maxAttempts: 5,
      timeoutMs: 30_000,
      maxRetryDelayMs: 60_000,
      delayMs: 0,
      metadata: { source: "test" },
    },
    parentId: null,
    rootId: id,
    depth: 0,
    ...extra,
  };
}

test("configuration is checked", () => {
  const ok = { apiKey: "rqh_x", endpoint: "https://data.example.com" };
  assert.equal(new CloudStorage(ok).queue, "default");
  for (const endpoint of [
    "",
    "http://data.example.com",
    "ftp://x",
    "https://u:p@x",
    "https://x/?a=1",
  ])
    assert.throws(() => new CloudStorage({ ...ok, endpoint }), /HTTPS/);
  assert.doesNotThrow(
    () => new CloudStorage({ ...ok, endpoint: "http://localhost:8081" }),
  );
  assert.throws(() => new CloudStorage({ ...ok, apiKey: "" }));
  assert.throws(() => new CloudStorage({ ...ok, queue: "has space" }));
  assert.throws(() => new CloudStorage({ ...ok, queue: "x".repeat(49) }));
});

test("calls carry the key, the version and Go-shaped arguments", async (t) => {
  const data = await fakeData(t, (c) => {
    switch (op(c)) {
      case "DequeueBatchEncoded":
        return {
          result: [
            {
              Activity: {
                ID: "a1",
                ActivityType: "charge",
                Payload: { json: { n: 1 }, meta: {} },
                Priority: 3,
                MaxRetries: 5,
                RetryCount: 1,
                TimeoutSeconds: 30,
                RetryDelaySeconds: 1,
                MaxRetryDelaySeconds: 60,
                ScheduledAt: "2026-09-29T11:59:00Z",
                Metadata: { source: "test" },
                IdempotencyKey: null,
                CreatedAt: "2026-09-29T11:00:00Z",
                ParentActivityID: null,
                RootActivityID: "a1",
                Depth: 0,
                Serialization: "superjson-v1",
              },
              LeaseID: "w:batch:x:a1",
              Attempt: 2,
              LeaseDeadline: "2026-09-29T12:01:00Z",
            },
          ],
        };
      case "AckFailure":
        return { result: true };
      case "ExtendLeaseForWorker":
        return { result: true };
      case "GetResult":
        return {
          result: {
            Data: { json: 1 },
            State: 0,
            Serialization: "superjson-v1",
          },
        };
      case "LookupIdempotencyActivityID":
        return { result: "a9" };
      default:
        return { result: null };
    }
  });
  const s = new CloudStorage({
    apiKey: "rqh_secret",
    endpoint: data.url + "/",
    queue: "payments",
  });

  const a = submission({
    serialization: "superjson-v1",
    payload: { json: 1, meta: {} },
  });
  assert.equal(await s.submit(a), a.id);
  const enqueue = data.calls.at(-1);
  assert.equal(enqueue.method, "POST");
  assert.equal(enqueue.path, "/v1/queues/payments/Enqueue");
  assert.equal(enqueue.headers.authorization, "Bearer rqh_secret");
  assert.equal(enqueue.headers["runnerq-storage-version"], "1");
  const q = enqueue.body.activity;
  assert.deepEqual(
    [
      q.ID,
      q.ActivityType,
      q.Priority,
      q.MaxRetries,
      q.TimeoutSeconds,
      q.RetryDelaySeconds,
      q.MaxRetryDelaySeconds,
      q.ScheduledAt,
      q.IdempotencyKey,
      q.RootActivityID,
      q.Serialization,
    ],
    [a.id, "charge", 3, 5, 30, 1, 60, null, null, a.id, "superjson-v1"],
  );
  assert.deepEqual(q.Metadata, { source: "test" });

  const claims = await s.claim(4, ["charge"], 60_000, "exec-1");
  const dq = data.calls.at(-1);
  assert.equal(op(dq), "DequeueBatchEncoded");
  assert.match(dq.body.workerIDPrefix, /^exec-1:batch:/);
  assert.deepEqual(dq.body.serializations, ["json-v1", "superjson-v1"]);
  assert.equal(dq.body.timeout, 0);
  assert.deepEqual(claims[0], {
    serialization: "superjson-v1",
    id: "a1",
    type: "charge",
    payload: { json: { n: 1 }, meta: {} },
    token: "w:batch:x:a1",
    retryCount: 1,
    timeoutMs: 30_000,
    parentId: null,
    rootId: "a1",
    depth: 0,
    metadata: { source: "test" },
    leaseDeadlineMs: Date.parse("2026-09-29T12:01:00Z"),
    dueAt: "2026-09-29T11:59:00Z",
  });

  const fence = { ownerId: "a1", token: "w:batch:x:a1" };
  assert.equal(await s.renew(fence, 60_000), true);
  assert.deepEqual(data.calls.at(-1).body, {
    activityID: "a1",
    workerID: fence.token,
    extendBy: 60_000_000_000,
  });

  await s.complete(fence, { data: { json: 2 }, serialization: "superjson-v1" });
  assert.deepEqual(data.calls.at(-1).body, {
    activityID: "a1",
    result: { json: 2 },
    serialization: "superjson-v1",
    workerID: fence.token,
  });

  const failure = { name: "RunnerQError", message: "slow", code: "timeout" };
  assert.equal(await s.fail(fence, "slow", true, failure), "dead_letter");
  assert.deepEqual(data.calls.at(-1).body.failure, {
    Retryable: true,
    Reason: "slow",
    IsTimeout: true,
    Details: failure,
  });
  assert.equal(await s.fail(fence, "bad", false), "failed");

  await s.checkpoint(
    fence,
    "c1",
    { state: "Err", data: { e: 1 }, serialization: "json-v1" },
    "run:load",
  );
  assert.deepEqual(data.calls.at(-1).body, {
    resultID: "c1",
    ownerID: "a1",
    workerID: fence.token,
    result: { Data: { e: 1 }, State: 1 },
    step: "run:load",
  });

  assert.deepEqual(await s.getResult("a1"), {
    state: "Ok",
    data: { json: 1 },
    serialization: "superjson-v1",
  });

  await s.park(fence, {
    kind: "await",
    step: "child",
    wakeAt: "2026-09-29T13:00:00.000Z",
    resultId: "r1",
    producerId: "p1",
  });
  assert.equal(op(data.calls.at(-1)), "YieldForResult");
  assert.deepEqual(data.calls.at(-1).body, {
    waiterID: "a1",
    resultID: "r1",
    producerID: "p1",
    wakeAt: "2026-09-29T13:00:00.000Z",
    workerID: fence.token,
    kind: "await",
    step: "child",
  });
  await s.park(fence, {
    kind: "sleep",
    step: "nap",
    wakeAt: "2026-09-29T13:00:00.000Z",
  });
  assert.equal(op(data.calls.at(-1)), "Yield");

  await s.registerDependency(fence, "p1");
  assert.deepEqual(data.calls.at(-1).body, {
    waiterID: "a1",
    resultID: "p1",
    workerID: fence.token,
  });

  const target = randomUUID();
  await s.signal(target, "approve", {
    data: { ok: true },
    serialization: "json-v1",
  });
  assert.deepEqual(data.calls.at(-1).body, {
    activityID: target,
    signalID: signalResultId(target, "approve"),
    name: "approve",
    payload: { ok: true },
    serialization: "json-v1",
  });

  assert.equal(await s.lookupKey("rq:key:v2:abc"), "a9");
  assert.equal(await s.reap(10), 0);
  await assert.rejects(
    s.cleanup({ completedMs: 1 }),
    /managed by RunnerQ Cloud/,
  );
});

test("children, idempotency and a lost submit reply", async (t) => {
  let refuse = false;
  const created = new Set();
  const data = await fakeData(t, (c) => {
    switch (op(c)) {
      case "EnqueueIdempotentForWorker":
        return {
          result: { ExistingID: "existing", ExistingParentID: "someone-else" },
        };
      case "Enqueue":
        if (refuse)
          return { error: { code: "invalid_argument", message: "no" } };
        created.add(c.body.activity.ID);
        return { status: 502, version: "" }; // committed, but the reply is lost
      case "GetActivity":
        return {
          result: created.has(c.body.activityID)
            ? { id: c.body.activityID }
            : null,
        };
      default:
        return { result: null };
    }
  });
  const s = new CloudStorage({ apiKey: "k", endpoint: data.url });

  const child = submission({
    parentId: "parent",
    key: "rq:step:r:p:charge",
    fence: { ownerId: "parent", token: "tok" },
  });
  assert.equal(await s.submit(child), "existing");
  const idem = data.calls.find((c) => op(c) === "EnqueueIdempotentForWorker");
  assert.deepEqual(
    [idem.body.ownerID, idem.body.workerID, idem.body.a.IdempotencyKey],
    ["parent", "tok", { Key: "rq:step:r:p:charge", Behavior: 1 }],
  );
  assert.deepEqual(data.calls.at(-1).body, {
    childID: "existing",
    parentID: "parent",
  }); // RecordSpawnLinked

  for (const [policy, n] of [
    ["allowReuse", 0],
    ["allowReuseOnFailure", 2],
    ["noReuse", 3],
  ]) {
    const a = submission({
      key: "k",
      options: {
        ...submission().options,
        idempotency: { key: "k", onDuplicate: policy },
      },
    });
    await s.submit(a);
    assert.equal(
      data.calls.find((c) => c.body?.activity?.ID === a.id).body.activity
        .IdempotencyKey.Behavior,
      n,
    );
  }

  // An Enqueue that commits but loses its reply is reconciled by id: the activity exists.
  const a = submission();
  assert.equal(await s.submit(a), a.id);
  assert.deepEqual(data.calls.slice(-2).map(op), ["Enqueue", "GetActivity"]);
  // One that really failed (nothing was created) still fails.
  refuse = true;
  const b = submission();
  await assert.rejects(
    s.submit(b),
    (e) => e instanceof RunnerQError && e.code === "configuration",
  );
});

test("errors map to the SDK's codes; redirects aren't followed", async (t) => {
  const data = await fakeData(t, (c) =>
    op(c) === "GetResult"
      ? { redirect: "http://elsewhere.invalid/" }
      : { error: { code: c.body.code, message: "no", field: c.body.field } },
  );
  const s = new CloudStorage({ apiKey: "k", endpoint: data.url });
  const cases = {
    claim_lost: "claim_lost",
    checkpoint_conflict: "checkpoint_conflict",
    duplicate_activity: "duplicate",
    idempotency_conflict: "idempotency_conflict",
    not_found: "not_found",
    unavailable: "unavailable",
    timeout: "timeout",
    invalid_argument: "configuration",
    something_new: "configuration",
  };
  for (const [wire, want] of Object.entries(cases)) {
    const e = await answeredWith(s, wire);
    assert.ok(e instanceof RunnerQError, wire);
    assert.equal(e.code, want, wire);
  }
  const withField = await answeredWith(s, "invalid_argument", "limit");
  assert.match(withField.message, /\(limit\)$/);
  const redirected = await s.getResult("a1").catch((e) => e);
  assert.ok(
    redirected instanceof RunnerQError && redirected.code === "unavailable",
  );
  assert.equal(data.calls.filter((c) => op(c) === "GetResult").length, 1);

  // Drives the client's request path; the fake answers with the code the body names.
  function answeredWith(storage, code, field) {
    return storage["call"]("Anything", { code, field }).catch((e) => e);
  }
});

test("waitForWork long-polls and the next claim takes what it found", async (t) => {
  let polls = 0;
  const data = await fakeData(t, async (c) => {
    if (op(c) !== "DequeueBatchEncoded") return { result: null };
    polls++;
    if (c.body.timeout === 0) return { result: [] };
    await new Promise((r) => setTimeout(r, 50));
    return {
      result: [
        {
          Activity: {
            ID: "late",
            ActivityType: "charge",
            Payload: 1,
            Priority: 2,
            MaxRetries: 0,
            RetryCount: 0,
            TimeoutSeconds: 30,
            RetryDelaySeconds: 1,
            MaxRetryDelaySeconds: 0,
            ScheduledAt: null,
            Metadata: null,
            IdempotencyKey: null,
            CreatedAt: "2026-09-29T12:00:00Z",
            ParentActivityID: null,
            RootActivityID: "late",
            Depth: 0,
          },
          LeaseID: "t",
          Attempt: 1,
          LeaseDeadline: "2026-09-29T12:01:00Z",
        },
      ],
    };
  });
  const s = new CloudStorage({ apiKey: "k", endpoint: data.url });
  assert.deepEqual(await s.claim(2, ["charge"], 60_000), []);
  await s.waitForWork(new AbortController().signal, 5_000);
  const poll = data.calls.at(-1);
  assert.equal(poll.body.timeout, 5_000_000_000);
  assert.equal(poll.body.limit, 2);
  const claimed = await s.claim(2, ["charge"], 60_000);
  assert.deepEqual(
    claimed.map((c) => [c.id, c.serialization, c.metadata]),
    [["late", "json-v1", {}]],
  );
  assert.equal(polls, 2, "the claim used what the poll found");

  const stop = new AbortController();
  const waiting = s.waitForWork(stop.signal, 20_000);
  stop.abort();
  await waiting; // an aborted wait just returns
});

test("a worker's executor is reported and says goodbye", async (t) => {
  const data = await fakeData(t);
  const s = new CloudStorage({
    apiKey: "rqh_k",
    endpoint: data.url,
    queue: "payments",
  });
  let changed;
  const source = {
    snapshot: () => ({
      info: {
        id: "exec-1",
        queue: "payments",
        activityTypes: ["charge"],
        maxConcurrency: 4,
        startedAt: new Date("2026-09-29T12:00:00Z"),
        hostname: "worker-a",
        sdk: { name: "runnerq-ts", version: "0.1.0", language: "typescript" },
        labels: { region: "eu" },
      },
      state: {
        running: [
          {
            id: "a1",
            type: "charge",
            attempt: 2,
            startedAt: new Date("2026-09-29T12:01:00Z"),
          },
        ],
        draining: false,
      },
      counters: {
        claimed: 5,
        succeeded: 3,
        retried: 1,
        failed: 0,
        timedOut: 0,
        deadLettered: 1,
        claimsLost: 0,
        heartbeatFailures: 2,
        lastClaimLagMs: 1500,
      },
      at: new Date("2026-09-29T12:02:00Z"),
    }),
    changed: () => new Promise((r) => (changed = r)),
  };
  s.executorStarted(source);
  const puts = () => data.calls.filter((c) => c.method === "PUT");
  while (!puts().length) await new Promise((r) => setTimeout(r, 5));
  const put = puts()[0];
  assert.equal(put.path, "/v1/executors/exec-1");
  assert.equal(put.headers.authorization, "Bearer rqh_k");
  assert.deepEqual(put.body.executor, {
    id: "exec-1",
    hostname: "worker-a",
    queues: ["payments"],
    activity_types: ["charge"],
    max_concurrency: 4,
    started_at: "2026-09-29T12:00:00.000Z",
    labels: { region: "eu" },
  });
  assert.equal(put.body.sdk.name, "runnerq-ts");
  const st = put.body.state;
  assert.deepEqual(
    [
      st.id,
      st.uptime_ms,
      st.in_flight,
      st.running[0].activity_id,
      st.running[0].started_at,
      st.claim_lag_ms,
      st.heartbeat_failures,
      st.counters.dead_lettered,
    ],
    ["exec-1", 120_000, 1, "a1", "2026-09-29T12:01:00.000Z", 1500, 2, 1],
  );

  await s.executorStopped("exec-1");
  const last = data.calls.at(-1);
  assert.deepEqual(
    [last.method, last.path],
    ["DELETE", "/v1/executors/exec-1"],
  );
  const n = data.calls.length;
  changed?.();
  await new Promise((r) => setTimeout(r, 50));
  assert.equal(data.calls.length, n, "nothing after the goodbye");
});

test("signal result ids match Go's uuid.NewSHA1", () => {
  // uuid.NewSHA1(uuid.MustParse(id), []byte("signal:approve")) in Go.
  assert.equal(
    signalResultId("6f1c2a3b-4d5e-4f60-8a7b-9c0d1e2f3a4b", "approve"),
    "3f68ce2b-48eb-561b-aeda-b103828772c2",
  );
});
