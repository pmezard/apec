package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func createTempQueue(t *testing.T) *IndexQueue {
	tmpDir, err := ioutil.TempDir("", "apec-")
	if err != nil {
		t.Fatalf("could not create temporary directory: %s", err)
	}
	path := filepath.Join(tmpDir, "queue")
	queue, err := OpenIndexQueue(path)
	if err != nil {
		t.Fatalf("could not open queue: %s", err)
	}
	return queue
}

func deleteTempQueue(t *testing.T, queue *IndexQueue) {
	queue.Close()
	os.RemoveAll(queue.Path())
}

func TestEmptyQueue(t *testing.T) {
	queue := createTempQueue(t)
	defer deleteTempQueue(t, queue)

	// Empty queue should be empty
	size := queue.Size()
	if size != 0 {
		t.Fatalf("empty queue is not empty: %d", size)
	}

	// Cannot fetch anything from empty queue
	queued, err := queue.FetchMany(10)
	if err != nil {
		t.Fatalf("failed to fetch from empty queue: %s", err)
	}
	if len(queued) != 0 {
		t.Fatalf("fetch items from empty queue: %v", queue)
	}

	// Can delete anything from empty queue
	err = queue.DeleteMany(3)
	if err != nil {
		t.Fatalf("cannot delete from empty queue: %s", err)
	}
}

func checkFetched(t *testing.T, queue *IndexQueue, wanted []Queued) {
	fmt.Println(wanted)
	for i := 0; i < len(wanted)+1; i++ {
		queued, err := queue.FetchMany(i)
		if err != nil {
			t.Fatalf("could not fetch %d items: %s", i, err)
		}
		n := i
		if n > len(wanted) {
			n = len(wanted)
		}
		if len(queued) != n {
			t.Fatalf("fetched %d, got %d", i, len(queued))
		}
		for j := 0; j < n; j++ {
			q := queued[j]
			w := wanted[j]
			w.Seq = q.Seq
			if w != q {
				t.Fatalf("items %d differ: %+v != %+v", j, w, q)
			}
		}
	}
}

func checkQueue(t *testing.T, queue *IndexQueue, entries []Queued, wanted []Queued) {
	size := queue.Size()
	err := queue.QueueMany(entries)
	if err != nil {
		t.Fatalf("cannot queue %d items: %s", len(entries), err)
	}
	if queue.Size() != (size + len(entries)) {
		t.Fatalf("queue size should be %d: %d", size+len(entries), queue.Size())
	}
	checkFetched(t, queue, wanted)
}

func checkDelete(t *testing.T, queue *IndexQueue, count int, wanted []Queued) {
	size := queue.Size()
	err := queue.DeleteMany(count)
	if err != nil {
		t.Fatalf("cannot delete %d items: %s", count, err)
	}
	size -= count
	if size < 0 {
		size = 0
	}
	if size != queue.Size() {
		t.Fatalf("queue size should be %d: %d", size, queue.Size())
	}
	checkFetched(t, queue, wanted)
}

func TestQueueOperations(t *testing.T) {
	queue := createTempQueue(t)
	defer deleteTempQueue(t, queue)

	err := queue.QueueMany(nil)
	if err != nil {
		t.Fatalf("cannot queue zero item: %s", err)
	}

	// Add 3 entries
	entries := []Queued{
		{Id: "0", Op: AddOp},
		{Id: "1", Op: RemoveOp},
		{Id: "2", Op: AddOp},
	}
	checkQueue(t, queue, entries, entries)

	// Remove less than available
	checkDelete(t, queue, 0, entries)
	entries = entries[2:]
	checkDelete(t, queue, 2, entries)

	// Add more entries and remove the exact amount
	added := []Queued{
		Queued{Id: "3", Op: RemoveOp},
	}
	entries = append(entries, added...)
	checkQueue(t, queue, added, entries)

	// Remove exact amount
	entries = entries[2:]
	checkDelete(t, queue, 2, entries)

	// Add again and remove more than added
	added = []Queued{
		Queued{Id: "4", Op: AddOp},
		Queued{Id: "5", Op: RemoveOp},
		Queued{Id: "6", Op: AddOp},
	}
	entries = append(entries, added...)
	checkQueue(t, queue, added, entries)
	entries = nil
	checkDelete(t, queue, 25, entries)

	err = queue.Close()
	if err != nil {
		t.Fatalf("could not close queue: %s", err)
	}
}
