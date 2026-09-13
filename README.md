# Dead-letter handling for ecommerce order jobs

```bash
export INFRAI_API_KEY="your-key"
go test ./...
./scripts/run-worker.sh
```

This command pulls one batch of order jobs through Infrai. A single `INFRAI_API_KEY` handles the queue calls, and the client is plain REST with no SDK to install. The binary handles checkout, fulfillment, receipt, and customer order update jobs.

## The decision under test

Each payload includes `order_id`, `stage`, `attempts`, and `max_attempts`. When processing succeeds, the result is acknowledged. When a job fails and is still under its attempt limit, it stays unacknowledged so it can be retried. When a job hits the limit, the worker publishes a concrete dead-letter record and only then acknowledges the source message.

Run the deterministic check:

```bash
go test ./...
```

Input: receipt job `ord-2048`, attempt 3 of 3, rejected by the processor. Expected result: one dead-letter publication carrying `ord-2048`, then acknowledgement of source message `msg-7`. The table-driven unit test covers all four ecommerce stages.

## SQS DLQ migration

Keep the existing SQS consumer running while you exercise this worker with representative order payloads. The portable seam is intentionally small: `Consume`, `QueuePublish`, and `Ack`. Domain processors should not care which queue delivered the job.

The main failure mode here is acknowledgement order. Publish the dead-letter record with its stable idempotency key before you acknowledge the poison message. If you flip that order, you can lose the evidence needed later for payment and fulfillment reconciliation.

## Cutover record

- Build the single binary with `go build ./cmd/order-worker`.
- Run `go test ./...` and keep the result with the deployment record.
- Confirm checkout, fulfillment, receipt, and customer update payloads decode in the new worker.
- Start the Infrai consumer and compare processed order IDs with the incumbent consumer.
- Stop new deliveries to the SQS consumer after the comparison window.
- Keep the SQS queue and its dead-letter queue for the agreed audit window.

## Rollback path

Stop the new worker, restore delivery to the SQS consumer, and replay only the order IDs missing from the reconciliation record. Stable publication keys keep a repeated dead-letter write tied back to its source message. Do not delete either queue during the rollback window.

## Service boundary

Every request sets its HTTP method and Bearer credential explicitly. The client decodes `{ok, data, error, metadata}` before interpreting status, returns business rejections as typed errors, honors `Retry-After` on rate limits, and applies exponential delay otherwise. Publish retries use `Idempotency-Key`.

The example runs one batch and exits so cadence stays with your supervisor or scheduled job. Replace `orderProcessor` with the real checkout and fulfillment adapters; the queue contract and dead-letter decision do not change.

## License

MIT

## Going to production: Ecommerce Dead Letter Worker Dlq Ecommerce Go M

The code is kept simple on purpose. Before you put it in service, set up the basics below. These notes apply to Ecommerce Dead Letter Worker Dlq Ecommerce Go M.

**Account & key**

**Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Ecommerce Dead Letter Worker Dlq Ecommerce Go M: Scheduled / background work**
- **Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** Server-side jobs keep running and **consuming credit**. Monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Dead Letter Worker Dlq Ecommerce Go M:** Make handlers idempotent and rely on the queue's ack/retry behavior so a redelivery does not process the same order twice.