package main

import (
	"log"

	"github.com/picatz/flowstate/pkg/flowstate/v1/engine"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create Temporal client.", err)
	}
	defer c.Close()

	w := worker.New(c, engine.RunTaskQueueName, worker.Options{})
	w.RegisterWorkflow(engine.Run)
	w.RegisterActivity(engine.Task)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("unable to start Worker", err)
	}
}
