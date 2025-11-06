# Go Workflow DAG Example

This is a Go implementation of a workflow DAG (Directed Acyclic Graph) that demonstrates the same ETL pipeline concepts as the Python Airflow DAG.

## Features

- **Task Orchestration**: Define tasks with dependencies
- **Retry Logic**: Automatic retries with configurable delays
- **Data Passing**: Share data between tasks (similar to XCom in Airflow)
- **Error Handling**: Comprehensive error handling and logging
- **Execution Summary**: View task execution times and statuses

## Structure

The DAG implements an ETL pipeline with the following tasks:

1. **Start** → Initializes the workflow
2. **Check Prerequisites** → Validates system requirements
3. **Extract** → Extracts data from source
4. **Transform** → Transforms the extracted data
5. **Load** → Loads data to destination
6. **Quality Check** → Validates data quality
7. **Cleanup** → Cleans up temporary resources
8. **End** → Completes the workflow

## How to Run

```bash
# Run the workflow
go run sample_workflow_dag.go
```

## Example Output

```
Starting DAG execution: sample_etl_pipeline - A sample ETL pipeline demonstrating workflow capabilities
Executing task: start - Start the workflow
Starting workflow...
Task completed: start (duration: 10ms)
Executing task: check_prerequisites - Check system prerequisites
Checking prerequisites...
Prerequisites OK
Task completed: check_prerequisites (duration: 200ms)
...
Workflow completed successfully

=== Execution Summary ===
Task: start                Status: completed  Duration: 10ms
Task: check_prerequisites  Status: completed  Duration: 200ms
Task: extract              Status: completed  Duration: 500ms
Task: transform            Status: completed  Duration: 500ms
Task: load                 Status: completed  Duration: 500ms
Task: quality_check        Status: completed  Duration: 200ms
Task: cleanup              Status: completed  Duration: 100ms
Task: end                  Status: completed  Duration: 5ms
```

## Comparison with Airflow

| Feature | Python Airflow | Go Implementation |
|---------|---------------|-------------------|
| Task Definition | Operators (PythonOperator, BashOperator) | Task struct with Execute function |
| Data Sharing | XCom | TaskResult with Data field |
| Dependencies | `>>` operator or set_downstream() | Dependencies array |
| Retry Logic | Built-in with default_args | Per-task Retries and RetryDelay |
| Scheduling | Cron-like expressions | Duration-based (can be extended) |
| Execution | Airflow scheduler | Direct execution in main() |

## Extending the Workflow

To add a new task:

```go
dag.AddTask(&Task{
    ID:           "my_task",
    Description:  "My custom task",
    Execute:      MyTaskFunction,
    Dependencies: []string{"previous_task"},
    Retries:      2,
    RetryDelay:   10 * time.Second,
})

func MyTaskFunction(ctx context.Context, inputs map[string]interface{}) (interface{}, error) {
    // Your task logic here
    return "result", nil
}
```

## Benefits of Go Implementation

- **Performance**: Native compilation and efficient concurrency
- **Type Safety**: Compile-time type checking
- **Single Binary**: Easy deployment without Python dependencies
- **Concurrency**: Built-in goroutines and channels (can be extended)
- **Memory Efficiency**: Lower memory footprint

## Next Steps

This implementation can be extended with:
- Parallel task execution using goroutines
- Database persistence for task results
- REST API for triggering workflows
- Advanced scheduling with cron expressions
- Metrics and monitoring integration
- Support for conditional task execution
