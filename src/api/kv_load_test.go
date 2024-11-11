package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"
)

func TestKVControllerLoadTest(t *testing.T) {
	//go test -v ./src/api -run TestKVControllerLoadTest
	baseURL := "http://localhost:9999"
	concurrentUsers := 50
	requestsPerUser := 100

	var wg sync.WaitGroup
	startTime := time.Now()

	// Create channels to collect metrics
	successChan := make(chan time.Duration, concurrentUsers*requestsPerUser*2)
	errorChan := make(chan error, concurrentUsers*requestsPerUser*2)

	// Add to WaitGroup for each concurrent user
	wg.Add(concurrentUsers)
	for i := 0; i < concurrentUsers; i++ {
		go func(userID int) {
			defer wg.Done() // Ensure that Done is called when goroutine finishes

			fmt.Printf("User %d started\n", userID)

			for j := 0; j < requestsPerUser; j++ {
				key := fmt.Sprintf("key-%d-%d", userID, j)

				// Test POST
				startPost := time.Now()
				kv := KV{
					Key:   key,
					Value: fmt.Sprintf("value-%d-%d", userID, j),
				}

				jsonData, _ := json.Marshal(kv)
				resp, err := http.Post(
					baseURL+"/v1/kv",
					"application/json",
					bytes.NewBuffer(jsonData),
				)

				if err != nil {
					errorChan <- fmt.Errorf("POST error: %v", err)
					continue
				}
				// Ensure that we don't close the response body too early
				resp.Body.Close()

				if resp.StatusCode != http.StatusCreated {
					errorChan <- fmt.Errorf("POST unexpected status: %d", resp.StatusCode)
					continue
				}

				successChan <- time.Since(startPost)
				fmt.Printf("POST took %v\n", time.Since(startPost))

				// Test GET
				startGet := time.Now()
				resp, err = http.Get(fmt.Sprintf("%s/v1/kv/%s", baseURL, key))

				if err != nil {
					errorChan <- fmt.Errorf("GET error: %v", err)
					continue
				}
				// Ensure that we don't close the response body too early
				resp.Body.Close()

				if resp.StatusCode != http.StatusOK {
					errorChan <- fmt.Errorf("GET unexpected status: %d", resp.StatusCode)
					continue
				}

				successChan <- time.Since(startGet)
				fmt.Printf("GET took %v\n", time.Since(startGet))
			}
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(successChan)
	close(errorChan)

	// Calculate metrics
	var totalDuration time.Duration
	successCount := 0
	errorCount := 0

	for successChan != nil || errorChan != nil {
		select {
		case duration, ok := <-successChan:
			if !ok {
				successChan = nil
				break
			}
			totalDuration += duration
			successCount++
		case err, ok := <-errorChan:
			if !ok {
				errorChan = nil
				break
			}
			errorCount++
			t.Logf("Error: %v", err)
			fmt.Println(err)
		}
	}

	totalTime := time.Since(startTime)
	totalRequests := successCount + errorCount
	var avgDuration time.Duration
	if successCount > 0 {
		avgDuration = totalDuration / time.Duration(successCount)
	}

	// Print results
	t.Logf("Load Test Results:")
	t.Logf("Total Time: %v", totalTime)
	t.Logf("Total Requests: %d", totalRequests)
	t.Logf("Successful Requests: %d", successCount)
	t.Logf("Failed Requests: %d", errorCount)
	t.Logf("Average Response Time: %v", avgDuration)
	t.Logf("Requests/Second: %.2f", float64(totalRequests)/totalTime.Seconds())
}
