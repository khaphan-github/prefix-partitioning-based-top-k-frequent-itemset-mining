# Parallel Processing Report

## Thiết lập

```bash
cd src
pip install -r requirments.txt
python3 main_with_args.py
```

# Configuration Documentation

## Overview
This document describes the benchmark configuration used in `main_with_args.py` for testing the PTF algorithm with different datasets and execution modes.

## Report Output Location
```python
REPORT_FOL = './benchmark/reports_01_01_v3'
```
All benchmark reports and metrics will be saved in this directory.

## Datasets

### 1. PUMSB Dataset
- **Name**: `pumsb`
- **File Path**: `./data/data_set/pumsb.txt`
- **Output Directory**: `./benchmark/reports_01_01_v3/pumsb_{runner_name}`
- **Top-K Values**: [10, 100, 1000, 5000, 10000]

### 2. OnlineRetailZZ Dataset
- **Name**: `OnlineRetailZZ`
- **File Path**: `./data/data_set/OnlineRetailZZ.txt`
- **Output Directory**: `./benchmark/reports_01_01_v3/OnlineRetailZZ_{runner_name}`
- **Top-K Values**: [10, 100, 1000, 5000, 10000]

## Runners

### 1. Sequential Runner
- **Name**: `sequential`
- **Description**: Sequential (single-threaded) execution of the PTF algorithm
- **Vietnamese**: Tuan tu (Tuần tự)

### 2. Parallel Co-Occurrent Runner
- **Name**: `parallel_co_occurrent`
- **Description**: Parallel execution with concurrent co-occurrence updates
- **Vietnamese**: XOn xong kha uopdate (Xong xong khả update)

## Execution Flow

The script will execute:
- **Total Runs**: 2 datasets × 5 top-k values × 2 runners = **20 benchmark runs**

For each combination:
1. Dataset is loaded from the specified file path
2. Algorithm runs with the specified top-k value
3. Results are saved to: `{output_dir}_{runner_name}/{runner_name}_report_{timestamp}.txt`
4. Metrics are saved to: `{output_dir}_{runner_name}/{runner_name}_metrics_{timestamp}.json`

## Example Output Paths

### For PUMSB with Sequential Runner (top-k=1000):
- Report: `./benchmark/reports_01_01_v3/pumsb_sequential/sequential_report_20260101_143025.txt`
- Metrics: `./benchmark/reports_01_01_v3/pumsb_sequential/sequential_metrics_20260101_143025.json`

### For OnlineRetailZZ with Parallel Co-Occurrent Runner (top-k=5000):
- Report: `./benchmark/reports_01_01_v3/OnlineRetailZZ_parallel_co_occurrent/parallel_co_occurrent_report_20260101_145530.txt`
- Metrics: `./benchmark/reports_01_01_v3/OnlineRetailZZ_parallel_co_occurrent/parallel_co_occurrent_metrics_20260101_145530.json`

## Running the Benchmarks

```bash
python3 main_with_args.py
```

The script will automatically execute all configured combinations sequentially.
