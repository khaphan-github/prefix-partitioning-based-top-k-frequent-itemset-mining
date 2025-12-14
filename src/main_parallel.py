#!/usr/bin/env python
"""Parallel PTF Algorithm"""

from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing


def main():
    dataset = "data/sample.txt"
    output = "ptf_parallel_output.txt"
    top_k = 8
    workers = 4
    print(f"Dataset: {dataset}, Top-k: {top_k}, Workers: {workers}")
    
    with open(output, "w") as f:
        exec_time = run_ptf_algorithm_parallel_with_timing(
            output_file=f,
            file_path=dataset,
            top_k=top_k,
            num_workers=workers
        )
    
    print(f"Completed in {exec_time:.4f} seconds")


if __name__ == "__main__":
    main()
