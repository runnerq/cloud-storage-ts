// End to end against a real storaged: set RUNNERQ_DATA_URL (e.g. http://localhost:8081)
// and RUNNERQ_DATA_ADMIN_TOKEN. The test provisions its own store and key.
import test from "node:test";
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import {
  Worker,
  RunnerQClient,
  activity,
  runner,
  NonRetryableError,
  ActivityFailedError,
} from "runnerq";
import { CloudStorage } from "../dist/index.js";

const endpoint = process.env.RUNNERQ_DATA_URL;
const adminToken = process.env.RUNNERQ_DATA_ADMIN_TOKEN;
async function admin(method, path, body) {
  const res = await fetch(endpoint + path, {
    method,
    headers: {
      Authorization: `Bearer ${adminToken}`,
      "Content-Type": "application/json",
    },
    body: body && JSON.stringify(body),
  });
  const out = await res.json();
  if (out.error) throw new Error(`${path}: ${out.error.message}`);
  return out.result;
}

test(
  "a TypeScript workflow on hosted storage",
  { skip: !endpoint || !adminToken, timeout: 90_000 },
  async () => {
    const { id: store } = await admin("POST", "/v1/admin/stores", {
      app_id: randomUUID(),
    });
    const { secret: apiKey } = await admin(
      "POST",
      `/v1/admin/stores/${store}/keys`,
      { name: "e2e" },
    );
    const storage = new CloudStorage({ apiKey, endpoint, queue: "orders" });
    const Charge = activity("charge"); // native: a Date round-trips
    const Refund = activity("refund");
    const Order = activity("order");
    const log = [];
    const worker = new Worker({
      storage,
      concurrency: 4,
      labels: { region: "local" },
      heartbeatMs: 2_000,
      leaseMs: 30_000,
    });
    worker.register(Charge, async (_, input) => {
      assert.ok(input.at instanceof Date, "native input keeps its Date");
      return { charged: input.amount, at: input.at, tags: new Set(["a", "b"]) };
    });
    worker.register(Refund, async () => {
      throw new NonRetryableError("refunds are closed");
    });
    worker.register(Order, async (ctx, input) => {
      const reserved = await ctx.run("reserve", async () => {
        log.push("reserve ran");
        return { ok: true, at: new Date("2026-09-29T10:00:00Z") };
      });
      const charge = await ctx.spawn(
        Charge,
        { amount: input.amount, at: new Date("2026-09-29T12:00:00Z") },
        runner.step("charge"),
      );
      const charged = await charge.result();
      await ctx.sleep("cool-off", 1_500); // longer than a quick wait: parks and wakes
      const approval = await ctx.waitForSignal("approve", {
        timeoutMs: 60_000,
      });
      let refund;
      try {
        await (await ctx.spawn(Refund, {}, runner.step("refund"))).result();
      } catch (e) {
        refund =
          e instanceof ActivityFailedError
            ? { failed: e.message }
            : { other: String(e) };
      }
      return { reserved, charged, approval, refund };
    });
    await worker.start();
    const client = new RunnerQClient({ storage });

    const order = await client.execute(
      Order,
      { amount: 42 },
      runner.idempotencyKey("order-42"),
    );
    const again = await client.execute(
      Order,
      { amount: 42 },
      runner.idempotencyKey("order-42"),
    );
    assert.equal(again.id, order.id, "the same key returns the existing order");

    // Signal once the order is parked on it.
    setTimeout(
      () =>
        client.signal(order.id, "approve", {
          by: "ops",
          at: new Date("2026-09-29T13:00:00Z"),
        }),
      3_000,
    );
    const result = await Promise.race([
      order.result(),
      new Promise((_, reject) =>
        setTimeout(
          () => reject(new Error("the order didn't finish in 60s")),
          60_000,
        ),
      ),
    ]);

    assert.equal(result.reserved.ok, true);
    assert.ok(
      result.reserved.at instanceof Date,
      "a step's native value round-trips",
    );
    assert.equal(result.charged.charged, 42);
    assert.ok(
      result.charged.at instanceof Date,
      "a child's native result round-trips",
    );
    assert.ok(
      result.charged.tags instanceof Set && result.charged.tags.has("b"),
      "a Set round-trips",
    );
    assert.equal(result.approval.by, "ops");
    assert.ok(
      result.approval.at instanceof Date,
      "a native signal payload round-trips",
    );
    assert.match(result.refund.failed ?? "", /refunds are closed/);
    assert.deepEqual(log, ["reserve ran"], "the step ran once despite replays");

    await worker.stop({ graceMs: 2_000 });
    await storage.close();

    // The worker reported itself, then said goodbye.
    const executors = await admin("GET", `/v1/admin/stores/${store}/executors`);
    const me = executors.find((e) => e.id === worker.id);
    assert.ok(me && !me.live && me.stopped_at, "reported, then stopped");
    assert.equal(me.report.sdk.name, "runnerq-ts");
    assert.deepEqual(me.report.executor.labels, { region: "local" });
  },
);
