// +build !appengine

package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

type t string

var (
	store       []t
	concurrency = flag.Int("concurrency", 1, "number of channels to fanout")
	items       = flag.Int("items", 10, "number of items to process")
)

func initStore(n int) {
	for i := 0; i < n; i++ {
		store = append(store, (t)(fmt.Sprintf("%d", i)))
	}
}

func main() {
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	initStore(*items)
	log.Printf("running pipeline with %d items and %d go routines on the write to archive stage", *items, *concurrency)
	ts := time.Now()
	run()
	tt := time.Since(ts)
	log.Printf("Total time elapsed: %s", tt)
}

// START PIPELINE OMIT
func run() {
	done := make(chan struct{}) // HL
	defer close(done)           // HL

	items := readDatabase()
	read := readArchive(items, done)
	written := writeToArchive(read, done)

	for item := range written {
		log.Printf("%s was processed", item)
	}
}

// END PIPELINE OMIT

// START READDB OMIT
func readDatabase() chan t {
	out := make(chan t) // HL
	go func(output chan<- t) {
		for _, item := range store {
			item = process(item, "database")
			output <- item // HL
		}
		close(output)
	}(out)
	return out // HL
}

// END READDB OMIT

// START READ ARCHIVE OMIT
func readArchive(work <-chan t, done <-chan struct{}) chan t {
	out := make(chan t, 50)
	go func(input <-chan t, output chan<- t, done <-chan struct{}) {
		defer close(out)
		for item := range input {
			item = process(item, "archive") // HL
			select {
			case output <- item: // HL
			case <-done: // HL
				return // HL
			}
		}
	}(work, out, done)
	return out
}

// END READ ARCHIVE OMIT

// START WRITE ARCHIVE OMIT
func writeToArchive(work <-chan t, done <-chan struct{}) chan t {
	out := make(chan t)
	go func() {
		defer close(out)
		fanout := make([]<-chan t, *concurrency) // HL
		for j := 0; j < *concurrency; j++ {
			fanout[j] = doWrite(work, done) // HL
		}

		for merged := range merge(fanout, done) { // HL
			select {
			case out <- merged:
			case <-done:
				return
			}
		}

	}()
	return out
}

// END WRITE ARCHIVE OMIT
// START MERGE OMIT
func merge(fanout []<-chan t, done <-chan struct{}) <-chan t {
	var wg sync.WaitGroup // HL
	wg.Add(len(fanout))
	out := make(chan t)
	process := func(ch <-chan t) { // HL
		defer wg.Done()
		for n := range ch {
			select {
			case out <- n:
			case <-done:
				return
			}
		}
	}
	for _, c := range fanout {
		go process(c) // HL
	}
	go func() {
		wg.Wait() // HL
		close(out)
	}()
	return out
}

// END MERGE OMIT
// START DO WRITE ARCHIVE OMIT
func doWrite(work <-chan t, done <-chan struct{}) chan t {
	out := make(chan t)
	go func() {
		defer close(out)
		for item := range work {
			item = process(item, "written")
			sleep := rand.Int63n(100)
			time.Sleep(time.Duration(sleep) * time.Millisecond)
			select {
			case out <- item:
			case <-done:
				return
			}
		}
	}()
	return out
}

// END DO WRITE ARCHIVE OMIT

func process(input, stage t) t {
	return (t)(fmt.Sprintf("%s|%s", input, stage))
}
