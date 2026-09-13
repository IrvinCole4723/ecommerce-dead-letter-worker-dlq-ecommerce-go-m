package orders

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestDecideOrderJob(t *testing.T) {
	tests := []struct {
		name string
		job  Job
		err  error
		want Decision
	}{
		{"checkout succeeds", Job{Stage: Checkout, Attempts: 1, MaxAttempts: 3}, nil, Process},
		{"fulfillment retries", Job{Stage: Fulfillment, Attempts: 2, MaxAttempts: 3}, errors.New("carrier rejected"), Retry},
		{"receipt is poison", Job{Stage: Receipt, Attempts: 3, MaxAttempts: 3}, errors.New("invalid receipt"), DeadLetter},
		{"order update is poison", Job{Stage: CustomerOrderUpdate, Attempts: 4, MaxAttempts: 4}, errors.New("invalid transition"), DeadLetter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Decide(tt.job, tt.err); got != tt.want {
				t.Fatalf("Decide() = %q, want %q", got, tt.want)
			}
		})
	}
}

type fakeQueue struct {
	messages  []Message
	published []DeadLetterRecord
	acked     []string
}

func (q *fakeQueue) Consume(context.Context, int, int) ([]Message, error) { return q.messages, nil }
func (q *fakeQueue) QueuePublish(_ context.Context, payload any, _ string) error {
	q.published = append(q.published, payload.(DeadLetterRecord))
	return nil
}
func (q *fakeQueue) Ack(_ context.Context, id string) error {
	q.acked = append(q.acked, id)
	return nil
}

type rejectingProcessor struct{}

func (rejectingProcessor) Process(context.Context, Job) error { return errors.New("rejected") }

func TestWorkerPublishesDeadLetterBeforeAck(t *testing.T) {
	payload, err := json.Marshal(Job{OrderID: "ord-2048", Stage: Receipt, Attempts: 3, MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	queue := &fakeQueue{messages: []Message{{MessageID: "msg-7", Payload: payload}}}
	worker := Worker{Queue: queue, Processor: rejectingProcessor{}}
	if err := worker.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(queue.published) != 1 || queue.published[0].Job.OrderID != "ord-2048" {
		t.Fatalf("published = %#v", queue.published)
	}
	if len(queue.acked) != 1 || queue.acked[0] != "msg-7" {
		t.Fatalf("acked = %#v", queue.acked)
	}
}
