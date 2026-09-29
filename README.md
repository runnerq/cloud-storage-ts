# @runnerq/cloud-storage

RunnerQ Cloud's hosted storage for the [RunnerQ TypeScript SDK](https://github.com/runnerq/runnerq-ts).
A worker built on it keeps no database of its own: every storage call goes to RunnerQ
Cloud's data plane over HTTPS with a store key, and scheduling, lease recovery and
retention run there.

```ts
import { Worker, RunnerQClient, activity } from "runnerq";
import { CloudStorage } from "@runnerq/cloud-storage";

const storage = new CloudStorage({
  apiKey: process.env.RUNNERQ_STORE_KEY!, // a store key (rqh_...) from the console
  endpoint: "https://data.runnerq.dev",
  queue: "orders", // created in the store on first use
});

const worker = new Worker({ storage, labels: { region: "eu-west-1" } });
worker.register(activity("charge"), async (ctx, input) => ({
  charged: input.amount,
}));
await worker.start();

const client = new RunnerQClient({ storage });
await client.execute(activity("charge"), { amount: 42 });
```

Everything the SDK does on PostgreSQL works the same here: durable steps, children,
sleeps, signals, idempotency keys, and native (`superjson-v1`) payloads and results, which
keep dates, maps, sets and bigints. A failure's details (name, stack, code, cause) are
kept for whoever awaits it.

- **Fleet:** a worker on this storage reports itself to RunnerQ Cloud every 10 seconds and
  within about two seconds of a change, and says goodbye when it stops, so the console
  shows it without a conductor agent.
- **Managed maintenance:** don't set `retention` on the worker; set it on the store in the
  console. Lease recovery (`reap`) runs in the data plane.
- **Safety:** the endpoint must be HTTPS (HTTP only on loopback). Redirects are never
  followed, so the key is never forwarded and a write is never replayed. A reply lost in
  transit surfaces as `unavailable`, which the SDK retries; an activity whose enqueue
  committed but lost its reply is recognised by its id.
- **Queues:** a store key reaches every queue in its store. Queue names are 1 to 48
  letters, digits or underscores, starting with a letter or underscore.

This needs a data plane that serves the encoded storage operations
(runnerq/runnerq-cloud's storaged, with runnerq-go's `EncodedStorage`).

## Development

```sh
npm install
npm test
```

`test/hosted.test.mjs` runs a whole workflow against a real data plane when
`RUNNERQ_DATA_URL` (e.g. `http://localhost:8081`) and `RUNNERQ_DATA_ADMIN_TOKEN` are set;
it provisions its own store and key. Without them it is skipped.
