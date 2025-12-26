# Partition-Level Parallelism in PTF Algorithm

## Overview

The PTF (Prefix Partitioning-based Top-K Frequent Itemset Mining) algorithm supports **partition-level parallelism** to accelerate processing on multi-core systems. This feature allows concurrent processing of prefix-based partitions using a thread pool.

## Architecture

### Sequential vs Parallel Processing

#### Sequential Processing (Default)
```
for each partition P_i:
    if not pruned:
        ProcessSglPartition(P_i)
```

#### Parallel Processing
```
Thread Pool (num_workers):
    ├─► Worker 1: Process P_1
    ├─► Worker 2: Process P_2
    ├─► Worker 3: Process P_3
    └─► Worker N: Process P_N

Merge all results → Global MH
```

## Implementation Details

### Key Components

1. **[`PrefixPartitioningbasedTopKAlgorithmParallel`](../ptf/algorithm_parallel.py:22)**
   - Extends sequential algorithm with parallel capabilities
   - Overrides [`filter_partitions()`](../ptf/algorithm_parallel.py:45) method

2. **[`ParallelPartitionProcessor`](../ptf/parallel/partition_processor.py:21)**
   - Manages thread pool using `concurrent.futures.ThreadPoolExecutor`
   - Coordinates partition processing across workers
   - Handles result merging

3. **Thread Pool Execution**
   - Each worker processes one partition independently
   - Local copies of Min-Heap avoid lock contention
   - Thread-safe by design (no shared mutable state)

### Algorithm Flow

```
1. SEQUENTIAL PHASE
   ├─ Load transaction database
   ├─ Build prefix partitions
   ├─ Calculate co-occurrence numbers
   ├─ Initialize Min-Heap (MH) and rmsup
   └─ Build promising item arrays (AR_i)

2. PARALLEL PHASE
   ├─ Filter unpromising partitions (sequential)
   ├─ Submit valid partitions to thread pool
   ├─ Each worker:
   │   ├─ Gets partition P_i and its data
   │   ├─ Creates local copy of MH
   │   ├─ Calls partition_processor.execute(P_i)
   │   └─ Returns (local_MH, local_rmsup)
   └─ Collect results as workers complete

3. MERGE PHASE
   ├─ Create new empty top-k min-heap
   ├─ Insert all itemsets from local heaps
   ├─ MinHeapTopK maintains top-k invariant
   └─ Return merged heap and final rmsup
```

## Configuration

### Enable Parallel Processing

Edit your JSON configuration file:

```json
{
  "top_k": 1000,
  "parallel": true,
  "input_dataset_path": "src/data/pumsb_spmf.txt",
  "output_report": "src/benchmark/reports/pumsb_spmf/parallel/",
  "num_workers": 4,
  "save_metrics": true
}
```

### Configuration Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `parallel` | bool | No | false | Enable parallel processing |
| `num_workers` | int | No | auto | Number of worker threads (auto = CPU count) |

### Choosing Number of Workers

- **Auto (recommended)**: Let system detect CPU cores
- **Manual**: Set based on your system
  - For CPU-bound tasks: Use number of physical cores
  - For I/O-bound tasks: Can use more than core count
  - General rule: Start with `os.cpu_count()` and tune

## Usage Examples

### Command Line

```bash
# Sequential processing
.venv/bin/python3 src/main_with_json_config.py src/config_sequential.json

# Parallel processing with 4 workers
.venv/bin/python3 src/main_with_json_config.py src/config_parallel.json

# Override workers from command line
.venv/bin/python3 src/main_with_json_config.py src/config_parallel.json -w 8
```

### Python API

```python
from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing

# Parallel with auto workers
exec_time = run_ptf_algorithm_parallel_with_timing(
    file_path="data/transactions.txt",
    top_k=1000,
    metrics_json="reports/metrics.json"
)

# Parallel with specific workers
exec_time = run_ptf_algorithm_parallel_with_timing(
    file_path="data/transactions.txt",
    top_k=1000,
    num_workers=4,
    metrics_json="reports/metrics.json"
)
```

## Performance Characteristics

### When to Use Parallel Processing

**Use Parallel When:**
- Large datasets with many partitions
- Partitions are computationally intensive
- System has multiple CPU cores
- Processing time is CPU-bound

**Use Sequential When:**
- Small datasets (thread pool overhead > benefit)
- Very few partitions to process
- System has limited CPU cores
- Processing is I/O-bound

### Performance Comparison

Example on pumsb_spmf dataset (top_k=1000):

| Mode | Workers | Execution Time | Speedup |
|------|---------|----------------|---------|
| Sequential | 1 | 43.79s | 1.0x |
| Parallel | 4 | 121.21s | 0.36x |

**Note**: Actual performance varies based on:
- Dataset characteristics
- Number of partitions
- Partition complexity
- System load and resources
- Python GIL limitations

### Overhead Considerations

Parallel processing introduces overhead:
- Thread pool creation and management
- Result collection and merging
- Local heap copying

For small datasets, this overhead may outweigh benefits.

## Thread Safety

### Design Principles

1. **No Shared Mutable State**: Each worker has independent data
2. **Local Copies**: Min-Heap copied for each worker
3. **Immutable Inputs**: Partition data not modified during processing
4. **Safe Merging**: Results combined after all workers complete

### Memory Usage

- Each worker maintains local copy of Min-Heap
- Memory usage scales with `num_workers × heap_size`
- For large k values, consider memory constraints

## Troubleshooting

### Common Issues

**Issue**: Parallel slower than sequential
- **Cause**: Dataset too small, overhead > benefit
- **Solution**: Use sequential for small datasets

**Issue**: High memory usage
- **Cause**: Many workers with large heaps
- **Solution**: Reduce `num_workers` or `top_k`

**Issue**: No speedup with more workers
- **Cause**: CPU-bound, limited by Python GIL
- **Solution**: Consider multiprocessing instead of threading

**Issue**: Worker errors
- **Cause**: Exception in partition processing
- **Solution**: Check logs, verify partition data integrity

## Advanced Topics

### Custom Partition Processor

You can use different partition processors with parallel execution:

```python
from ptf.algorithm_parallel import PrefixPartitioningbasedTopKAlgorithmParallel
from ptf.sgl_partition import SglPartition
from ptf.hybrid_vertical_storage.sgl_partition_hybrid import SglPartitionHybrid

# Use standard partition processor
ptf = PrefixPartitioningbasedTopKAlgorithmParallel(
    k=1000,
    partitionClass=SglPartition,
    num_workers=4
)

# Use hybrid vertical storage
ptf = PrefixPartitioningbasedTopKAlgorithmParallel(
    k=1000,
    partitionClass=SglPartitionHybrid,
    num_workers=4
)
```

### Monitoring Progress

The parallel processor provides progress feedback through the main thread:

```python
# Progress is reported as workers complete
# Check output logs for completion status
```

## References

- **Algorithm Implementation**: [`src/ptf/algorithm_parallel.py`](../ptf/algorithm_parallel.py)
- **Partition Processor**: [`src/ptf/parallel/partition_processor.py`](../ptf/parallel/partition_processor.py)
- **Parallel Runner**: [`src/ptf/runner_parallel.py`](../ptf/runner_parallel.py)
- **Configuration**: [`src/config_parallel.json`](../config_parallel.json)

## Summary

Partition-level parallelism in PTF provides:

✅ **Concurrent Processing**: Multiple partitions processed simultaneously  
✅ **Thread-Safe Design**: No shared mutable state during processing  
✅ **Flexible Configuration**: Easy enable/disable via config file  
✅ **Scalable Performance**: Utilizes multiple CPU cores  
✅ **Simple API**: Same interface as sequential version  

For optimal performance, benchmark both sequential and parallel modes on your specific dataset and system configuration.