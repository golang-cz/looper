package main

import (
	"context"
	"log"
	"time"

	"github.com/golang-cz/looper"
)

func main() {
	l := looper.New(nil)

	j := &looper.Job{
		JobFn:            job,
		Name:             "jj",
		Timeout:          10 * time.Second,
		WaitAfterSuccess: 4 * time.Second,
		WaitAfterError:   4 * time.Second,
		Active:           true,
	}

	ctx := context.Background()

	if err := l.AddJob(ctx, j); err != nil {
		log.Fatal(err)
	}

	l.Start()

	select {}
}

func job(ctx context.Context) error {
	log.Println("job start")

	time.Sleep(2 * time.Second)

	log.Println("job end")

	return nil
}
