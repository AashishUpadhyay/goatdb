# 30 Sept 2024

- Staring this project called Goat DB
- At this point I want to create a new no SQL DB that is optimized for writes and works well in a single node
- Using the folowing DBs for reference: badger, junodb and dolt.
- DB is an abstraction on Txn
- The Open function in DB is long running function that keeps running and writes data to disk
- Try creating a simple DB in golang that is capable of reading\writing data in-memory tomorrow

# 1 Oct 2024

- Create db.go with two operations Get and Put that allow adding and retrieving a KV pair

# 2 Oct 2024

- DB only accepts an entry where Key is string and Value is byte[]. Consumer is expected to encode the values to binary format before saving

# 3 Oct 2024

- DB saves the value to a file and reads it. Each KV is written to the sstable file on disk in a new line
- Performance needs to be evaluated.
- Next steps:
  - Create two APIs, GET and PUT
  - Benchmark both APIs
- Here is some information from ChantGPT on how to go about it:

```
To measure the performance of your NoSQL KV-style database in Golang, you can follow these steps:

1. Set up APIs for Get and Put
You can expose Get and Put methods for your key-value store using HTTP or gRPC APIs to simulate how real-world applications interact with the database. Below is a simple HTTP-based structure.

2. Use Benchmarking and Profiling Tools
To measure performance from the start, use Go's built-in testing tools like testing.Benchmark, pprof, and others.

Example KV Database Structure with Basic Performance Measurement
Here’s a basic example to get you started:

a) KV Store with Get and Put Operations:
go
Copy code
package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
	"encoding/json"
	"log"
)

type KVStore struct {
	store map[string]string
	mu    sync.RWMutex
}

func NewKVStore() *KVStore {
	return &KVStore{
		store: make(map[string]string),
	}
}

// Put method to insert key-value
func (kv *KVStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[key] = value
}

// Get method to retrieve value by key
func (kv *KVStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.store[key]
	return value, exists
}

// HTTP Handlers for Put and Get

func (kv *KVStore) PutHandler(w http.ResponseWriter, r *http.Request) {
	var body map[string]string
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	key := body["key"]
	value := body["value"]
	start := time.Now()
	kv.Put(key, value)
	duration := time.Since(start)
	fmt.Fprintf(w, "Put operation took %s\n", duration)
}

func (kv *KVStore) GetHandler(w http.ResponseWriter, r *http.Request) {
	keys, ok := r.URL.Query()["key"]
	if !ok || len(keys[0]) < 1 {
		http.Error(w, "Missing key", http.StatusBadRequest)
		return
	}
	key := keys[0]
	start := time.Now()
	value, exists := kv.Get(key)
	duration := time.Since(start)
	if !exists {
		http.Error(w, "Key not found", http.StatusNotFound)
		return
	}
	fmt.Fprintf(w, "Value: %s, Get operation took %s\n", value, duration)
}

func main() {
	kv := NewKVStore()

	http.HandleFunc("/put", kv.PutHandler)
	http.HandleFunc("/get", kv.GetHandler)

	fmt.Println("Server is running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
The PutHandler measures the time taken for each Put operation, and GetHandler does the same for Get.
You can use curl or any HTTP client to test performance (curl -X POST localhost:8080/put -d '{"key":"test", "value":"value"}').
3. Benchmarking Using Go’s testing Package
You can use the built-in Go testing package to create benchmarks and profile performance.

Example of Benchmarking Get and Put:
Create a separate _test.go file for your benchmarking tests:

go
Copy code
package main

import (
	"testing"
)

func BenchmarkPut(b *testing.B) {
	kv := NewKVStore()
	for i := 0; i < b.N; i++ {
		kv.Put("key"+string(i), "value")
	}
}

func BenchmarkGet(b *testing.B) {
	kv := NewKVStore()
	kv.Put("key", "value")

	for i := 0; i < b.N; i++ {
		kv.Get("key")
	}
}
Run the benchmarks using:

bash
Copy code
go test -bench=.
The results will show you how many operations per second are happening and how long they take on average.

4. Profiling with pprof
You can also integrate pprof to track memory, CPU usage, and contention. Add the following line to your main():

go
Copy code
import _ "net/http/pprof"

go func() {
    log.Println(http.ListenAndServe("localhost:6060", nil))
}()
Then, you can collect profiling data:

CPU Profile: go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
Heap Profile: go tool pprof http://localhost:6060/debug/pprof/heap
Analyze performance bottlenecks using tools like Flamegraphs or pprof visualizations.

5. Performance Metrics to Monitor
For Get and Put operations, monitor the following:

Latency: Measure the time taken for each operation.
Throughput: Measure the number of operations per second.
Concurrency: See how the system performs under concurrent load (e.g., multiple Get and Put requests).
Memory Usage: Monitor how memory grows as you store more data in the KV store.
CPU Usage: Profile CPU usage for the operations, especially under high load.
6. Load Testing with Tools like wrk or ab
Use external tools like wrk or ab to generate load on your APIs and measure performance under stress.

Example with wrk:
bash
Copy code
wrk -t12 -c400 -d30s http://localhost:8080/get?key=test
This command runs wrk with 12 threads, 400 concurrent connections, for 30 seconds, hitting your Get API.

Summary
Step 1: Implement Get and Put APIs.
Step 2: Use Go's built-in testing package to benchmark the operations.
Step 3: Add profiling with pprof for deeper analysis.
Step 4: Perform load testing using external tools like wrk or ab.
By combining benchmarking, profiling, and load testing, you can gain insights into the performance of your database's Get and Put operations under various conditions.
```
