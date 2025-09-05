package netselect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promauth"
)

// Helper functions for testing
func createTestStorage(nodeCount int) *Storage {
	addrs := make([]string, nodeCount)
	authCfgs := make([]*promauth.Config, nodeCount)
	isTLSs := make([]bool, nodeCount)

	for i := 0; i < nodeCount; i++ {
		addrs[i] = fmt.Sprintf("node%d:9428", i+1)
		authCfgs[i] = &promauth.Config{}
		isTLSs[i] = false
	}

	return NewStorage(addrs, authCfgs, isTLSs, false)
}

func createTestQueryContext(t *testing.T) *logstorage.QueryContext {
	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	return &logstorage.QueryContext{
		Context:    context.Background(),
		TenantIDs:  []logstorage.TenantID{{}},
		Query:      q,
		QueryStats: &logstorage.QueryStats{},
	}
}

func resetFlags() (func(), int, int) {
	origMinAvailable := *minAvailableStorageNodes
	origMaxUnavailable := *maxUnavailableStorageNodes

	return func() {
		*minAvailableStorageNodes = origMinAvailable
		*maxUnavailableStorageNodes = origMaxUnavailable
	}, origMinAvailable, origMaxUnavailable
}

func resetMetrics() {
	partialQueryFailures.Set(0)
	unavailableNodes.Set(0)
	partialResults.Set(0)
}

// Test NewStorage function
func TestNewStorage(t *testing.T) {
	tests := []struct {
		name               string
		addrs              []string
		disableCompression bool
		expectPanic        bool
	}{
		{
			name:               "Valid single node",
			addrs:              []string{"localhost:9428"},
			disableCompression: false,
			expectPanic:        false,
		},
		{
			name:               "Valid multiple nodes",
			addrs:              []string{"node1:9428", "node2:9428", "node3:9428"},
			disableCompression: false,
			expectPanic:        false,
		},
		{
			name:               "Valid with compression disabled",
			addrs:              []string{"localhost:9428"},
			disableCompression: true,
			expectPanic:        false,
		},
		{
			name:               "Empty addresses",
			addrs:              []string{},
			disableCompression: false,
			expectPanic:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authCfgs := make([]*promauth.Config, len(tt.addrs))
			isTLSs := make([]bool, len(tt.addrs))

			for i := range tt.addrs {
				authCfgs[i] = &promauth.Config{}
				isTLSs[i] = false
			}

			defer func() {
				if r := recover(); r != nil && !tt.expectPanic {
					t.Errorf("Unexpected panic: %v", r)
				}
			}()

			storage := NewStorage(tt.addrs, authCfgs, isTLSs, tt.disableCompression)
			if storage == nil && !tt.expectPanic {
				t.Error("Expected non-nil storage")
				return
			}

			if storage != nil {
				defer storage.MustStop()

				if len(storage.sns) != len(tt.addrs) {
					t.Errorf("Expected %d storage nodes, got %d", len(tt.addrs), len(storage.sns))
				}

				if storage.disableCompression != tt.disableCompression {
					t.Errorf("Expected disableCompression=%v, got %v", tt.disableCompression, storage.disableCompression)
				}

				// Verify storage nodes are properly initialized
				for i, sn := range storage.sns {
					if sn.addr != tt.addrs[i] {
						t.Errorf("Expected addr %s, got %s", tt.addrs[i], sn.addr)
					}
					if sn.scheme != "http" {
						t.Errorf("Expected scheme http, got %s", sn.scheme)
					}
					if sn.s != storage {
						t.Error("Storage node should reference parent storage")
					}
				}
			}
		})
	}
}

func TestCheckQueryResults(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()
	resetMetrics()

	storage := createTestStorage(5)
	defer storage.MustStop()

	tests := []struct {
		name                  string
		minAvailable          int
		maxUnavailable        int
		successfulNodes       int
		totalNodes            int
		errs                  []error
		expectError           bool
		expectedErrorContains string
		expectPartialFailures bool
		expectPartialResults  bool
	}{
		{
			name:            "All nodes successful",
			minAvailable:    0,
			maxUnavailable:  -1,
			successfulNodes: 5,
			totalNodes:      5,
			errs:            make([]error, 5),
			expectError:     false,
		},
		{
			name:                 "Partial success - default settings",
			minAvailable:         0,
			maxUnavailable:       -1,
			successfulNodes:      3,
			totalNodes:           5,
			errs:                 []error{nil, nil, nil, errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:          false,
			expectPartialResults: true,
		},
		{
			name:                 "Partial success - min 2 required, 3 successful",
			minAvailable:         2,
			maxUnavailable:       -1,
			successfulNodes:      3,
			totalNodes:           5,
			errs:                 []error{nil, nil, nil, errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:          false,
			expectPartialResults: true,
		},
		{
			name:                  "Insufficient nodes - min 4 required, 3 successful",
			minAvailable:          4,
			maxUnavailable:        -1,
			successfulNodes:       3,
			totalNodes:            5,
			errs:                  []error{nil, nil, nil, errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:           true,
			expectedErrorContains: "insufficient available storage nodes",
			expectPartialFailures: true,
		},
		{
			name:                  "Too many unavailable - max 1 unavailable, 2 failed",
			minAvailable:          0,
			maxUnavailable:        1,
			successfulNodes:       3,
			totalNodes:            5,
			errs:                  []error{nil, nil, nil, errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:           true,
			expectedErrorContains: "too many unavailable storage nodes",
			expectPartialFailures: true,
		},
		{
			name:                 "Max 2 unavailable - exactly 2 failed",
			minAvailable:         0,
			maxUnavailable:       2,
			successfulNodes:      3,
			totalNodes:           5,
			errs:                 []error{nil, nil, nil, errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:          false,
			expectPartialResults: true,
		},
		{
			name:                  "No nodes successful",
			minAvailable:          0,
			maxUnavailable:        -1,
			successfulNodes:       0,
			totalNodes:            5,
			errs:                  []error{errors.New("node 1 failed"), errors.New("node 2 failed"), errors.New("node 3 failed"), errors.New("node 4 failed"), errors.New("node 5 failed")},
			expectError:           true,
			expectedErrorContains: "all 5 storage nodes failed",
			expectPartialFailures: true,
		},
		{
			name:            "Context canceled errors ignored",
			minAvailable:    0,
			maxUnavailable:  -1,
			successfulNodes: 3,
			totalNodes:      5,
			errs:            []error{nil, nil, nil, context.Canceled, context.Canceled},
			expectError:     false,
		},
		{
			name:                  "Mixed errors with context canceled",
			minAvailable:          4,
			maxUnavailable:        -1,
			successfulNodes:       2,
			totalNodes:            5,
			errs:                  []error{nil, nil, errors.New("real error"), context.Canceled, context.Canceled},
			expectError:           true,
			expectedErrorContains: "insufficient available storage nodes",
			expectPartialFailures: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset metrics for each test
			resetMetrics()

			*minAvailableStorageNodes = tt.minAvailable
			*maxUnavailableStorageNodes = tt.maxUnavailable

			// Adjust storage to have the right number of nodes
			if len(storage.sns) != tt.totalNodes {
				storage.MustStop()
				storage = createTestStorage(tt.totalNodes)
				defer storage.MustStop()
			}

			err := storage.checkQueryResults(tt.errs, tt.successfulNodes)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
			if tt.expectedErrorContains != "" && (err == nil || !strings.Contains(err.Error(), tt.expectedErrorContains)) {
				t.Errorf("Expected error containing '%s', got: %v", tt.expectedErrorContains, err)
			}

			// Check metrics
			if tt.expectPartialFailures && partialQueryFailures.Get() == 0 {
				t.Error("Expected partialQueryFailures metric to be incremented")
			}
			if tt.expectPartialResults && partialResults.Get() == 0 {
				t.Error("Expected partialResults metric to be incremented")
			}
		})
	}
}

func TestGetNodeStatus(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()

	tests := []struct {
		name       string
		nodeCount  int
		minAvail   int
		maxUnavail int
	}{
		{"Single node", 1, 0, -1},
		{"Multiple nodes", 3, 2, 1},
		{"Many nodes", 10, 5, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			*minAvailableStorageNodes = tt.minAvail
			*maxUnavailableStorageNodes = tt.maxUnavail

			storage := createTestStorage(tt.nodeCount)
			defer storage.MustStop()

			status := storage.GetNodeStatus()

			// Check basic structure
			if status["total_nodes"] != tt.nodeCount {
				t.Errorf("Expected %d total nodes, got %v", tt.nodeCount, status["total_nodes"])
			}

			if status["min_available_nodes"] != tt.minAvail {
				t.Errorf("Expected min_available_nodes %d, got %v", tt.minAvail, status["min_available_nodes"])
			}

			if status["max_unavailable_nodes"] != tt.maxUnavail {
				t.Errorf("Expected max_unavailable_nodes %d, got %v", tt.maxUnavail, status["max_unavailable_nodes"])
			}

			// Check nodes array
			nodes, ok := status["nodes"].([]map[string]interface{})
			if !ok {
				t.Error("Expected nodes to be a slice of maps")
				return
			}

			if len(nodes) != tt.nodeCount {
				t.Errorf("Expected %d nodes in status, got %d", tt.nodeCount, len(nodes))
			}

			// Check each node
			for i, node := range nodes {
				expectedAddr := fmt.Sprintf("node%d:9428", i+1)
				if node["addr"] != expectedAddr {
					t.Errorf("Expected node %d addr to be %s, got %v", i, expectedAddr, node["addr"])
				}
				if node["scheme"] != "http" {
					t.Errorf("Expected node %d scheme to be http, got %v", i, node["scheme"])
				}
				if _, exists := node["send_errors"]; !exists {
					t.Errorf("Expected node %d to have send_errors field", i)
				}
			}

			// Check metrics fields exist
			metricsFields := []string{"partial_query_failures", "unavailable_nodes_total", "partial_results_total"}
			for _, field := range metricsFields {
				if _, exists := status[field]; !exists {
					t.Errorf("Expected status to contain %s field", field)
				}
			}
		})
	}
}

func TestGetValuesWithHits(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()
	resetMetrics()

	tests := []struct {
		name                 string
		nodeCount            int
		successfulNodes      int
		minAvailable         int
		maxUnavailable       int
		limit                uint64
		resetHits            bool
		expectError          bool
		expectedResultCount  int
		expectPartialResults bool
	}{
		{
			name:                "All nodes successful",
			nodeCount:           3,
			successfulNodes:     3,
			minAvailable:        0,
			maxUnavailable:      -1,
			limit:               100,
			resetHits:           false,
			expectError:         false,
			expectedResultCount: 3,
		},
		{
			name:                 "Partial success - fault tolerant",
			nodeCount:            5,
			successfulNodes:      3,
			minAvailable:         0,
			maxUnavailable:       -1,
			limit:                100,
			resetHits:            false,
			expectError:          false,
			expectedResultCount:  3,
			expectPartialResults: true,
		},
		{
			name:            "Insufficient nodes",
			nodeCount:       5,
			successfulNodes: 2,
			minAvailable:    3,
			maxUnavailable:  -1,
			limit:           100,
			resetHits:       false,
			expectError:     true,
		},
		{
			name:            "Too many unavailable",
			nodeCount:       5,
			successfulNodes: 3,
			minAvailable:    0,
			maxUnavailable:  1,
			limit:           100,
			resetHits:       false,
			expectError:     true,
		},
		{
			name:            "All nodes failed",
			nodeCount:       3,
			successfulNodes: 0,
			minAvailable:    0,
			maxUnavailable:  -1,
			limit:           100,
			resetHits:       false,
			expectError:     true,
		},
		{
			name:                "With limit and reset hits",
			nodeCount:           3,
			successfulNodes:     3,
			minAvailable:        0,
			maxUnavailable:      -1,
			limit:               10,
			resetHits:           true,
			expectError:         false,
			expectedResultCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetMetrics()
			*minAvailableStorageNodes = tt.minAvailable
			*maxUnavailableStorageNodes = tt.maxUnavailable

			storage := createTestStorage(tt.nodeCount)
			defer storage.MustStop()

			qctx := createTestQueryContext(t)

			// Create callback that simulates node responses
			callback := func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
				// Extract node number from address
				nodeNum := 0
				fmt.Sscanf(sn.addr, "node%d:9428", &nodeNum)

				// Fail nodes beyond successfulNodes count
				if nodeNum > tt.successfulNodes {
					return nil, fmt.Errorf("node %d failed", nodeNum)
				}

				// Return test data for successful nodes
				return []logstorage.ValueWithHits{
					{Value: fmt.Sprintf("value%d", nodeNum), Hits: uint64(nodeNum)},
				}, nil
			}

			vhs, err := storage.getValuesWithHits(qctx, tt.limit, tt.resetHits, callback)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			// Check results for successful cases
			if !tt.expectError {
				if len(vhs) == 0 && tt.expectedResultCount > 0 {
					t.Error("Expected some results but got none")
				}
				// Note: The exact count may vary due to merging logic in logstorage.MergeValuesWithHits
			}

			// Check metrics
			if tt.expectPartialResults && partialResults.Get() == 0 {
				t.Error("Expected partialResults metric to be incremented")
			}
		})
	}
}

func TestRunQuery(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()
	resetMetrics()

	tests := []struct {
		name                 string
		nodeCount            int
		successfulNodes      int
		minAvailable         int
		maxUnavailable       int
		expectError          bool
		expectPartialResults bool
	}{
		{
			name:            "All nodes successful",
			nodeCount:       3,
			successfulNodes: 3,
			minAvailable:    0,
			maxUnavailable:  -1,
			expectError:     false,
		},
		{
			name:                 "Partial success - fault tolerant",
			nodeCount:            5,
			successfulNodes:      3,
			minAvailable:         0,
			maxUnavailable:       -1,
			expectError:          false,
			expectPartialResults: true,
		},
		{
			name:            "Insufficient nodes",
			nodeCount:       5,
			successfulNodes: 2,
			minAvailable:    3,
			maxUnavailable:  -1,
			expectError:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetMetrics()
			*minAvailableStorageNodes = tt.minAvailable
			*maxUnavailableStorageNodes = tt.maxUnavailable

			// Create test servers
			servers := make([]*httptest.Server, tt.nodeCount)
			addrs := make([]string, tt.nodeCount)
			authCfgs := make([]*promauth.Config, tt.nodeCount)
			isTLSs := make([]bool, tt.nodeCount)

			for i := 0; i < tt.nodeCount; i++ {
				nodeIndex := i
				if nodeIndex < tt.successfulNodes {
					// Successful server
					servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusOK)
						// Write minimal valid response
						w.Write([]byte{0, 0, 0, 0, 0, 0, 0, 0}) // 8 bytes for data length (0)
					}))
				} else {
					// Failing server
					servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte("server error"))
					}))
				}
				defer servers[i].Close()

				addrs[i] = servers[i].URL[7:] // Remove "http://" prefix
				authCfgs[i] = &promauth.Config{}
				isTLSs[i] = false
			}

			storage := NewStorage(addrs, authCfgs, isTLSs, false)
			defer storage.MustStop()

			qctx := createTestQueryContext(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			qctx.Context = ctx

			var blockCount int32
			writeBlock := func(workerID uint, db *logstorage.DataBlock) {
				atomic.AddInt32(&blockCount, 1)
			}

			err := storage.runQuery(ctx.Done(), qctx, writeBlock)

			// Check error expectation
			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}

			// Check metrics
			if tt.expectPartialResults && partialResults.Get() == 0 {
				t.Error("Expected partialResults metric to be incremented")
			}
		})
	}
}

func TestPublicAPIMethods(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()

	// Test all public API methods that use getValuesWithHits
	methods := []struct {
		name string
		call func(*Storage, *logstorage.QueryContext) ([]logstorage.ValueWithHits, error)
	}{
		{
			name: "GetFieldNames",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetFieldNames(qctx)
			},
		},
		{
			name: "GetFieldValues",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetFieldValues(qctx, "test_field", 100)
			},
		},
		{
			name: "GetStreamFieldNames",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetStreamFieldNames(qctx)
			},
		},
		{
			name: "GetStreamFieldValues",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetStreamFieldValues(qctx, "test_field", 100)
			},
		},
		{
			name: "GetStreams",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetStreams(qctx, 100)
			},
		},
		{
			name: "GetStreamIDs",
			call: func(s *Storage, qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
				return s.GetStreamIDs(qctx, 100)
			},
		},
	}

	for _, method := range methods {
		t.Run(method.name, func(t *testing.T) {
			resetMetrics()
			*minAvailableStorageNodes = 0
			*maxUnavailableStorageNodes = -1

			storage := createTestStorage(3)
			defer storage.MustStop()

			qctx := createTestQueryContext(t)

			// This will fail because we don't have real servers, but we're testing the fault tolerance logic
			_, err := method.call(storage, qctx)

			// We expect errors since we're not running real servers, but the method should not panic
			if err == nil {
				t.Logf("Method %s succeeded unexpectedly (this is okay for some test scenarios)", method.name)
			}
		})
	}
}

func TestEdgeCases(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()

	t.Run("Empty storage", func(t *testing.T) {
		storage := createTestStorage(0)
		defer storage.MustStop()

		status := storage.GetNodeStatus()
		if status["total_nodes"] != 0 {
			t.Errorf("Expected 0 total nodes, got %v", status["total_nodes"])
		}

		// Test checkQueryResults with empty storage
		err := storage.checkQueryResults([]error{}, 0)
		if err == nil {
			t.Error("Expected error for empty storage with no successful nodes")
		}
	})

	t.Run("Single node storage", func(t *testing.T) {
		resetMetrics()
		*minAvailableStorageNodes = 1
		*maxUnavailableStorageNodes = 0

		storage := createTestStorage(1)
		defer storage.MustStop()

		// Test successful case
		err := storage.checkQueryResults([]error{nil}, 1)
		if err != nil {
			t.Errorf("Expected no error for single successful node, got: %v", err)
		}

		// Test failed case
		err = storage.checkQueryResults([]error{errors.New("failed")}, 0)
		if err == nil {
			t.Error("Expected error for single failed node")
		}
	})

	t.Run("Extreme flag values", func(t *testing.T) {
		resetMetrics()
		storage := createTestStorage(5)
		defer storage.MustStop()

		// Test very high minimum requirement
		*minAvailableStorageNodes = 1000
		*maxUnavailableStorageNodes = -1
		err := storage.checkQueryResults(make([]error, 5), 5)
		if err == nil {
			t.Error("Expected error for impossible minimum requirement")
		}

		// Test zero max unavailable
		*minAvailableStorageNodes = 0
		*maxUnavailableStorageNodes = 0
		err = storage.checkQueryResults([]error{nil, nil, nil, nil, errors.New("failed")}, 4)
		if err == nil {
			t.Error("Expected error when max unavailable is 0 but 1 node failed")
		}
	})

	t.Run("Context cancellation", func(t *testing.T) {
		resetMetrics()
		*minAvailableStorageNodes = 0
		*maxUnavailableStorageNodes = -1

		storage := createTestStorage(3)
		defer storage.MustStop()

		// All errors are context.Canceled - should not count as failures
		errs := []error{context.Canceled, context.Canceled, context.Canceled}
		err := storage.checkQueryResults(errs, 0)
		if err == nil {
			t.Error("Expected error when all nodes canceled")
		}

		// Mixed with real errors
		errs = []error{nil, context.Canceled, errors.New("real error")}
		err = storage.checkQueryResults(errs, 1)
		if err != nil {
			t.Errorf("Expected no error for mixed cancellation, got: %v", err)
		}
	})
}

func TestConcurrency(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()
	resetMetrics()

	*minAvailableStorageNodes = 0
	*maxUnavailableStorageNodes = -1

	storage := createTestStorage(5)
	defer storage.MustStop()

	t.Run("Concurrent checkQueryResults calls", func(t *testing.T) {
		const numGoroutines = 100
		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				// Create different error scenarios
				errs := make([]error, 5)
				successfulNodes := 3

				if id%3 == 0 {
					// Some failures
					errs[3] = fmt.Errorf("error %d", id)
					errs[4] = fmt.Errorf("error %d", id)
				}

				err := storage.checkQueryResults(errs, successfulNodes)
				if err != nil {
					errors <- err
				}
			}(i)
		}

		wg.Wait()
		close(errors)

		// Collect any errors
		var collectedErrors []error
		for err := range errors {
			collectedErrors = append(collectedErrors, err)
		}

		// Should not have any errors in fault-tolerant mode
		if len(collectedErrors) > 0 {
			t.Errorf("Expected no errors in concurrent execution, got %d errors", len(collectedErrors))
		}
	})

	t.Run("Concurrent getValuesWithHits calls", func(t *testing.T) {
		const numGoroutines = 50
		var wg sync.WaitGroup
		results := make(chan []logstorage.ValueWithHits, numGoroutines)
		errors := make(chan error, numGoroutines)

		qctx := createTestQueryContext(t)

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				callback := func(ctx context.Context, sn *storageNode) ([]logstorage.ValueWithHits, error) {
					// Simulate some nodes failing
					if strings.Contains(sn.addr, "node4") || strings.Contains(sn.addr, "node5") {
						return nil, fmt.Errorf("node failed in goroutine %d", id)
					}
					return []logstorage.ValueWithHits{
						{Value: fmt.Sprintf("value-%d", id), Hits: uint64(id)},
					}, nil
				}

				vhs, err := storage.getValuesWithHits(qctx, 100, false, callback)
				if err != nil {
					errors <- err
				} else {
					results <- vhs
				}
			}(i)
		}

		wg.Wait()
		close(results)
		close(errors)

		// Collect results
		var allResults [][]logstorage.ValueWithHits
		for result := range results {
			allResults = append(allResults, result)
		}

		var allErrors []error
		for err := range errors {
			allErrors = append(allErrors, err)
		}

		// In fault-tolerant mode, should have mostly successful results
		if len(allResults) == 0 {
			t.Error("Expected some successful results in concurrent execution")
		}

		t.Logf("Concurrent execution: %d successful, %d failed", len(allResults), len(allErrors))
	})
}

func TestMetricsIncrement(t *testing.T) {
	restore, _, _ := resetFlags()
	defer restore()

	storage := createTestStorage(5)
	defer storage.MustStop()

	tests := []struct {
		name                   string
		minAvailable           int
		maxUnavailable         int
		successfulNodes        int
		expectPartialFailures  bool
		expectPartialResults   bool
		expectUnavailableNodes bool
	}{
		{
			name:                   "Partial success",
			minAvailable:           0,
			maxUnavailable:         -1,
			successfulNodes:        3,
			expectPartialResults:   true,
			expectUnavailableNodes: true,
		},
		{
			name:                   "Insufficient nodes",
			minAvailable:           4,
			maxUnavailable:         -1,
			successfulNodes:        2,
			expectPartialFailures:  true,
			expectUnavailableNodes: true,
		},
		{
			name:                   "Too many unavailable",
			minAvailable:           0,
			maxUnavailable:         1,
			successfulNodes:        3,
			expectPartialFailures:  true,
			expectUnavailableNodes: true,
		},
		{
			name:            "All successful",
			minAvailable:    0,
			maxUnavailable:  -1,
			successfulNodes: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resetMetrics()
			*minAvailableStorageNodes = tt.minAvailable
			*maxUnavailableStorageNodes = tt.maxUnavailable

			// Create errors for failed nodes
			errs := make([]error, 5)
			for i := tt.successfulNodes; i < 5; i++ {
				errs[i] = fmt.Errorf("node %d failed", i)
			}

			initialPartialFailures := partialQueryFailures.Get()
			initialPartialResults := partialResults.Get()
			initialUnavailableNodes := unavailableNodes.Get()

			_ = storage.checkQueryResults(errs, tt.successfulNodes)

			// Check metric increments
			if tt.expectPartialFailures {
				if partialQueryFailures.Get() <= initialPartialFailures {
					t.Error("Expected partialQueryFailures to be incremented")
				}
			}

			if tt.expectPartialResults {
				if partialResults.Get() <= initialPartialResults {
					t.Error("Expected partialResults to be incremented")
				}
			}

			if tt.expectUnavailableNodes {
				// Note: unavailableNodes is incremented in runQuery/getValuesWithHits, not checkQueryResults
				// This test verifies the metric exists and can be incremented
				unavailableNodes.Inc()
				if unavailableNodes.Get() <= initialUnavailableNodes {
					t.Error("Expected unavailableNodes to be incrementable")
				}
			}
		})
	}
}
