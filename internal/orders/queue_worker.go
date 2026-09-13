package orders

import (
	"context"
	"encoding/json"
	"fmt"
)

type Stage string

const (
	Checkout            Stage = "checkout"
	Fulfillment         Stage = "fulfillment"
	Receipt             Stage = "receipt"
	CustomerOrderUpdate Stage = "customer_order_update"
)

type Job struct {
	OrderID     string `json:"order_id"`
	Stage       Stage  `json:"stage"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
}

type Decision string

const (
	Process    Decision = "process"
	Retry      Decision = "retry"
	DeadLetter Decision = "dead_letter"
)

func Decide(job Job, processingErr error) Decision {
	if processingErr == nil {
		return Process
	}
	if job.MaxAttempts > 0 && job.Attempts >= job.MaxAttempts {
		return DeadLetter
	}
	return Retry
}

type Processor interface {
	Process(context.Context, Job) error
}

type Queue interface {
	Consume(context.Context, int, int) ([]Message, error)
	QueuePublish(context.Context, any, string) error
	Ack(context.Context, string) error
}

type Worker struct {
	Queue     Queue
	Processor Processor
}

type DeadLetterRecord struct {
	Disposition string `json:"disposition"`
	SourceID    string `json:"source_message_id"`
	Job         Job    `json:"job"`
}

func (w Worker) RunOnce(ctx context.Context) error {
	messages, err := w.Queue.Consume(ctx, 10, 30)
	if err != nil {
		return err
	}
	for _, message := range messages {
		if err := w.handle(ctx, message); err != nil {
			return err
		}
	}
	return nil
}

func (w Worker) handle(ctx context.Context, message Message) error {
	var job Job
	if err := json.Unmarshal(message.Payload, &job); err != nil {
		return w.deadLetter(ctx, message.MessageID, job)
	}
	processingErr := w.Processor.Process(ctx, job)
	switch Decide(job, processingErr) {
	case Process:
		return w.Queue.Ack(ctx, message.MessageID)
	case DeadLetter:
		return w.deadLetter(ctx, message.MessageID, job)
	case Retry:
		return nil
	default:
		return fmt.Errorf("unknown order decision")
	}
}

func (w Worker) deadLetter(ctx context.Context, messageID string, job Job) error {
	record := DeadLetterRecord{Disposition: "dead_letter", SourceID: messageID, Job: job}
	if err := w.Queue.QueuePublish(ctx, record, "dead-letter-"+messageID); err != nil {
		return err
	}
	return w.Queue.Ack(ctx, messageID)
}
