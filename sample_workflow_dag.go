package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// TaskStatus represents the status of a task
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
)

// TaskResult represents the output of a task
type TaskResult struct {
	TaskID    string
	Status    TaskStatus
	Data      interface{}
	Error     error
	StartTime time.Time
	EndTime   time.Time
}

// Task represents a unit of work in the DAG
type Task struct {
	ID           string
	Description  string
	Execute      func(ctx context.Context, inputs map[string]interface{}) (interface{}, error)
	Dependencies []string
	Retries      int
	RetryDelay   time.Duration
}

// DAG represents a Directed Acyclic Graph of tasks
type DAG struct {
	ID          string
	Description string
	Tasks       map[string]*Task
	Results     map[string]*TaskResult
	Schedule    time.Duration
}

// NewDAG creates a new DAG instance
func NewDAG(id, description string) *DAG {
	return &DAG{
		ID:          id,
		Description: description,
		Tasks:       make(map[string]*Task),
		Results:     make(map[string]*TaskResult),
	}
}

// AddTask adds a task to the DAG
func (d *DAG) AddTask(task *Task) {
	d.Tasks[task.ID] = task
}

// Execute runs the DAG
func (d *DAG) Execute(ctx context.Context) error {
	log.Printf("Starting DAG execution: %s - %s", d.ID, d.Description)

	// Track completed tasks
	completed := make(map[string]bool)

	// Execute tasks in dependency order
	for len(completed) < len(d.Tasks) {
		// Find tasks ready to execute
		for taskID, task := range d.Tasks {
			if completed[taskID] {
				continue
			}

			// Check if all dependencies are completed
			if d.areDependenciesCompleted(task, completed) {
				if err := d.executeTask(ctx, task); err != nil {
					return fmt.Errorf("task %s failed: %w", taskID, err)
				}
				completed[taskID] = true
			}
		}
	}

	log.Printf("DAG execution completed: %s", d.ID)
	return nil
}

// areDependenciesCompleted checks if all task dependencies are completed
func (d *DAG) areDependenciesCompleted(task *Task, completed map[string]bool) bool {
	for _, depID := range task.Dependencies {
		if !completed[depID] {
			return false
		}
	}
	return true
}

// executeTask executes a single task with retry logic
func (d *DAG) executeTask(ctx context.Context, task *Task) error {
	result := &TaskResult{
		TaskID:    task.ID,
		Status:    StatusRunning,
		StartTime: time.Now(),
	}

	log.Printf("Executing task: %s - %s", task.ID, task.Description)

	// Gather inputs from dependencies
	inputs := make(map[string]interface{})
	for _, depID := range task.Dependencies {
		if depResult, ok := d.Results[depID]; ok {
			inputs[depID] = depResult.Data
		}
	}

	// Execute with retries
	var err error
	var data interface{}

	for attempt := 0; attempt <= task.Retries; attempt++ {
		if attempt > 0 {
			log.Printf("Retrying task %s (attempt %d/%d)", task.ID, attempt, task.Retries)
			time.Sleep(task.RetryDelay)
		}

		data, err = task.Execute(ctx, inputs)
		if err == nil {
			break
		}

		if attempt == task.Retries {
			result.Status = StatusFailed
			result.Error = err
			result.EndTime = time.Now()
			d.Results[task.ID] = result
			return err
		}
	}

	result.Status = StatusCompleted
	result.Data = data
	result.EndTime = time.Now()
	d.Results[task.ID] = result

	log.Printf("Task completed: %s (duration: %v)", task.ID, result.EndTime.Sub(result.StartTime))
	return nil
}

// GetTaskResult retrieves the result of a task (similar to XCom in Airflow)
func (d *DAG) GetTaskResult(taskID string) (interface{}, error) {
	if result, ok := d.Results[taskID]; ok {
		if result.Status == StatusCompleted {
			return result.Data, nil
		}
		return nil, fmt.Errorf("task %s not completed", taskID)
	}
	return nil, fmt.Errorf("task %s not found", taskID)
}

// ============================================================================
// ETL Pipeline Tasks (equivalent to the Python DAG)
// ============================================================================

// ExtractData simulates data extraction
func ExtractData(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Extracting data from source...")
	time.Sleep(500 * time.Millisecond) // Simulate work

	data := map[string]interface{}{
		"records": 100,
		"status":  "success",
	}

	log.Printf("Extracted data: %v", data)
	return data, nil
}

// TransformData simulates data transformation
func TransformData(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Transforming data...")
	time.Sleep(500 * time.Millisecond) // Simulate work

	// Get data from extract task
	extractData, ok := inputs["extract"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid input from extract task")
	}

	records := extractData["records"].(int)
	transformedRecords := records * 2

	result := map[string]interface{}{
		"transformed_records": transformedRecords,
	}

	log.Printf("Transformed data: %v", result)
	return result, nil
}

// LoadData simulates data loading
func LoadData(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Loading data to destination...")
	time.Sleep(500 * time.Millisecond) // Simulate work

	transformData, ok := inputs["transform"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid input from transform task")
	}

	records := transformData["transformed_records"]
	log.Printf("Loading %v records", records)

	return "Load completed successfully", nil
}

// CheckPrerequisites simulates prerequisite checks
func CheckPrerequisites(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Checking prerequisites...")
	time.Sleep(200 * time.Millisecond)
	log.Println("Prerequisites OK")
	return "OK", nil
}

// QualityCheck simulates data quality validation
func QualityCheck(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Running quality checks...")
	time.Sleep(200 * time.Millisecond)
	log.Println("Quality checks passed")
	return "PASSED", nil
}

// Cleanup simulates cleanup operations
func Cleanup(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Cleaning up temporary files...")
	time.Sleep(100 * time.Millisecond)
	return "Cleanup complete", nil
}

// Start is a dummy operator for workflow start
func Start(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Starting workflow...")
	return "Started", nil
}

// End is a dummy operator for workflow end
func End(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
	log.Println("Workflow completed successfully")
	return "Completed", nil
}

// ============================================================================
// Main - Build and Execute the DAG
// ============================================================================

func main() {
	// Create the DAG
	dag := NewDAG("sample_etl_pipeline", "A sample ETL pipeline demonstrating workflow capabilities")

	// Define tasks with dependencies (matching the Python DAG structure)
	dag.AddTask(&Task{
		ID:          "start",
		Description: "Start the workflow",
		Execute:     Start,
		Retries:     1,
		RetryDelay:  5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "check_prerequisites",
		Description:  "Check system prerequisites",
		Execute:      CheckPrerequisites,
		Dependencies: []string{"start"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "extract",
		Description:  "Extract data from source",
		Execute:      ExtractData,
		Dependencies: []string{"check_prerequisites"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "transform",
		Description:  "Transform extracted data",
		Execute:      TransformData,
		Dependencies: []string{"extract"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "load",
		Description:  "Load data to destination",
		Execute:      LoadData,
		Dependencies: []string{"transform"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "quality_check",
		Description:  "Validate data quality",
		Execute:      QualityCheck,
		Dependencies: []string{"load"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "cleanup",
		Description:  "Cleanup temporary resources",
		Execute:      Cleanup,
		Dependencies: []string{"quality_check"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	dag.AddTask(&Task{
		ID:           "end",
		Description:  "End the workflow",
		Execute:      End,
		Dependencies: []string{"cleanup"},
		Retries:      1,
		RetryDelay:   5 * time.Second,
	})

	// Execute the DAG
	ctx := context.Background()
	if err := dag.Execute(ctx); err != nil {
		log.Fatalf("DAG execution failed: %v", err)
	}

	// Print execution summary
	fmt.Println("\n=== Execution Summary ===")
	for taskID, result := range dag.Results {
		duration := result.EndTime.Sub(result.StartTime)
		fmt.Printf("Task: %-20s Status: %-10s Duration: %v\n", taskID, result.Status, duration)
	}
}
