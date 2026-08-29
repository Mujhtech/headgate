package main

import (
	"encoding/json"
	"fmt"

	headgate "github.com/mujhtech/headgate/go"
	"github.com/mujhtech/headgate/go/headgateworkflow"
)

type importTask struct {
	Stage string `json:"stage"`
}

func (importTask) Kind() string { return "example:daily-import" }

func envelope(stage string) (headgate.Envelope, error) {
	payload, err := json.Marshal(importTask{Stage: stage})
	if err != nil {
		return headgate.Envelope{}, err
	}
	return headgate.Envelope{
		Kind:          importTask{}.Kind(),
		Payload:       payload,
		Queue:         "imports",
		SchemaVersion: 1,
	}, nil
}

func run() error {
	extract, err := envelope("extract")
	if err != nil {
		return err
	}
	customers, err := envelope("customers")
	if err != nil {
		return err
	}
	orders, err := envelope("orders")
	if err != nil {
		return err
	}
	index, err := envelope("index")
	if err != nil {
		return err
	}

	batch, err := headgateworkflow.New("daily-import-2026-08-28").
		CoordinatorQueue("workflows").
		Add("extract", extract).
		Add("customers", customers, "extract").
		Add("orders", orders, "extract").
		Add("index", index, "customers", "orders").
		Prepare()
	if err != nil {
		return err
	}
	if len(batch) != 5 || batch[0].Kind != headgateworkflow.CoordinatorKind {
		return fmt.Errorf("unexpected workflow batch: %#v", batch)
	}
	for i, job := range batch {
		if i > 0 && !job.Pending {
			return fmt.Errorf("child %s was not prepared as pending", job.ID)
		}
		fmt.Printf("%-42s kind=%-24s queue=%-10s pending=%t\n", job.ID, job.Kind, job.Queue, job.Pending)
	}
	fmt.Println("fan-out/fan-in workflow prepared as one atomic enqueue batch")
	return nil
}

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}
