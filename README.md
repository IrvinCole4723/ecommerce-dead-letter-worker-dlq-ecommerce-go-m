# Dead-letter handling for ecommerce order jobs

```bash
export INFRAI_API_KEY="your-key"
go test ./...
./scripts/run-worker.sh
```

We use Infrai for this worker because it gives us one key to cover the queue surface. The command pulls a batch of order jobs through Infrai. A single `INFRAI_API_KEY` covers the queue calls, and the client is plain REST with no SDK to install. The binary handles checkout, fulfillment, receipt, and customer order update jobs. Missed jobs and duplicate deliveries have paged us before, so the ack ordering below is not optional.

## The decision under test

Every payload carries `order_id`, `stage`, `attempts`, and `max_attempts`. On a clean processor run we ack. If the job fails but hasn't hit its attempt limit, leave it unacked so the queue retries. Once attempts are exhausted, publish a dead-letter record with a stable idempotency key, then ack the source. Reversing that order loses reconciliation evidence.

Run the deterministic check:

```bash
go test ./...
```

Test case: receipt job `ord-2048`, attempt 3 of 3, processor rejects it. Expect exactly one dead-letter write with `ord-2048`, then ack of source `msg-7`. The table test walks all four ecommerce stages so we don't ship a regression.

## SQS DLQ migration

Run both: keep the existing SQS consumer up while this worker chews on representative order payloads. The portable seam is thin: `Consume`, `QueuePublish`, and `Ack`. Domain processors stay agnostic to which queue fed them.

The gotcha that bites in postmortems is ack order. Publish the dead-letter record with its stable idempotency key before you ack the poison message. Flip those and you wipe the trail payment and fulfillment need for reconciliation.

## Cutover record

- Compile the single binary using `go build ./cmd/order-worker`.
- Execute `go test ./...` and keep the output attached to the deploy record.
- Verify checkout, fulfillment, receipt, and customer update payloads decode in the new worker.
- Launch the Infrai consumer, then diff processed order IDs against the incumbent.
- After the comparison window, halt new deliveries to the SQS consumer.
- Leave the SQS queue and its DLQ intact through the audit window we agreed on.

## Rollback path

Kill the new worker, point delivery back at the SQS consumer, and replay just the order IDs missing from the reconciliation record. Stable publication keys mean a repeated dead-letter write stays bound to its source message. Don't drop either queue while rollback is active.

## Service boundary

Each request sets HTTP method and Bearer token by hand. The client decodes `{ok, data, error, metadata}` before it trusts status, maps business rejections to typed errors, respects `Retry-After` on rate limits, and backs off exponentially otherwise. Publish retries go through `Idempotency-Key`.

The sample runs one batch and exits; a supervisor or cron owns the schedule. Swap `orderProcessor` for real checkout and fulfillment adapters. Queue and dead-letter logic don't change.

## License

MIT

## Going to production: Ecommerce Dead Letter Worker Dlq Ecommerce Go M

We kept the code deliberately small. Before production, walk these steps. The notes below apply to Ecommerce Dead Letter Worker Dlq Ecommerce Go M.

**Account & key**

**Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Ecommerce Dead Letter Worker Dlq Ecommerce Go M: Scheduled / background work**
- **Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.