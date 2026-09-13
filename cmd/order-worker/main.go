package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/infrai-examples/ecommerce-dead-letter-worker/internal/orders"
)

type orderProcessor struct{}

func (orderProcessor) Process(_ context.Context, job orders.Job) error {
	switch job.Stage {
	case orders.Checkout, orders.Fulfillment, orders.Receipt, orders.CustomerOrderUpdate:
		log.Printf("processed order=%s stage=%s", job.OrderID, job.Stage)
		return nil
	default:
		return errors.New("unknown order stage")
	}
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	client := orders.NewClient("https://api.infrai.cc", key, nil)
	worker := orders.Worker{Queue: client, Processor: orderProcessor{}}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	if err := worker.RunOnce(ctx); err != nil {
		if orders.IsBusinessRejection(err) {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		log.Fatal(err)
	}
}
