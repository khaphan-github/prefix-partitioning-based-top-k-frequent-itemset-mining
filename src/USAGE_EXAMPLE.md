# Simple Usage Examples

## 1. Sequential PTF with JSON Config

### Create config file: `config.json`
```json
{
  "top_k": 8,
  "parallel": false,
  "input_dataset_path": "data/transactions.txt",
  "output_report": "reports/output.txt",
  "save_metrics": true
}
```

### Run
```bash
python3 main_with_json_config.py config_sequential.json
```

### Output
```
Loading configuration from: config.json

Configuration loaded successfully:
{
  "top_k": 8,
  "parallel": false,
  "input_dataset_path": "data/transactions.txt",
  "output_report": "reports/output.txt",
  "save_metrics": true
}

Running Sequential PTF...
Sequential execution completed in 1.2345 seconds

Results saved to: reports/output.txt
Metrics saved to: reports/output_metrics.json

✓ Algorithm execution completed successfully
```

---

## 2. Parallel PTF with JSON Config

### Create config file: `config_parallel.json`
```json
{
  "top_k": 8,
  "parallel": true,
  "input_dataset_path": "data/transactions.txt",
  "output_report": "reports/parallel_output.txt",
  "num_workers": 4,
  "save_metrics": true
}
```

### Run
```bash
python src/main_with_json_config.py config_parallel.json
```

---

## 3. Command-line Overrides

Run with config but override parameters:

```bash
# Override top_k
python src/main_with_json_config.py config.json -k 10

# Override output path
python src/main_with_json_config.py config.json -o results/custom_output.txt

# Force parallel execution
python src/main_with_json_config.py config.json -p --workers 8
```

---

## 4. Config File Examples

### Minimal (Sequential)
```json
{
  "top_k": 8,
  "input_dataset_path": "data/transactions.txt",
  "output_report": "output.txt"
}
```

### Full (Parallel with all options)
```json
{
  "top_k": 10,
  "parallel": true,
  "input_dataset_path": "data/my_transactions.txt",
  "output_report": "reports/results.txt",
  "num_workers": 4,
  "save_metrics": true
}
```

---

## 5. Generated Output Files

### Report File (`output.txt`)
```
======================================================================
PTF Algorithm Report
Generated: 2025-12-14T10:30:00.123456
======================================================================

Configuration:
  Algorithm: Sequential PTF
  Top-k: 8
  Input Dataset: data/transactions.txt

Initial rmsup: 100
Initial MH size: 8

Total itemsets found: 8
Final rmsup: 5

1. {1, 2}             => Support: 150
2. {1, 3}             => Support: 145
3. {2, 3}             => Support: 140
...

Execution time: 1234.50 ms
Memory used: 256500.00 KB
```

### Metrics File (`output_metrics.json`)
```json
{
  "timestamp": "2025-12-14T10:30:00.123456",
  "algorithm": "PTF",
  "parameters": {
    "top_k": 8
  },
  "metrics": {
    "execution_time_ms": 1234.50,
    "memory_used_kb": 256500.00
  }
}
```

---

## 6. Python API (Alternative)

Instead of JSON config file, use directly in Python:

```python
from ptf.runner import run_ptf_algorithm_with_timing
from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing

# Sequential
run_ptf_algorithm_with_timing(
    file_path="data/transactions.txt",
    top_k=8,
    metrics_json="reports/metrics.json"
)

# Parallel
run_ptf_algorithm_parallel_with_timing(
    file_path="data/transactions.txt",
    top_k=8,
    num_workers=4,
    metrics_json="reports/metrics.json"
)
```

---

## Configuration Reference

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `top_k` | int | Yes | - | Number of top-k itemsets to find |
| `parallel` | bool | No | false | Use parallel execution |
| `input_dataset_path` | str | Yes | - | Path to transaction database |
| `output_report` | str | Yes | - | Path to save report |
| `num_workers` | int | No | auto | Threads for parallel (only if parallel=true) |
| `save_metrics` | bool | No | true | Save metrics to JSON |

---

## Troubleshooting

### Config file not found
```
Error: Config file not found: config.json
```
**Solution:** Check file path is correct and file exists

### Input dataset not found
```
Error: Input dataset not found: data/transactions.txt
```
**Solution:** Ensure input_dataset_path points to existing file

### Invalid JSON
```
Error: Invalid JSON in config file: ...
```
**Solution:** Validate JSON syntax in config file

### Missing required field
```
Error: Missing required field: input_dataset_path
```
**Solution:** Ensure all required fields are in config JSON
