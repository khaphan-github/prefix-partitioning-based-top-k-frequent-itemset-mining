#!/usr/bin/env python
"""Parallel PTF Algorithm"""

from ptf.utils import ExecutionTimer, show_progress
from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing


def main():
    dataset = "data/sample.txt"
    output = "ptf_parallel_output.txt"
    workers = 4
    
    with ExecutionTimer("PTF Algorithm Parallel - All Test Cases"):
        report_file = open(output, "w")
        
        test_cases = [
            (8, "Test Case 1: top_k=8"),
            (5, "Test Case 2: top_k=5"),
            (10, "Test Case 3: top_k=10"),
            (3, "Test Case 4: top_k=3"),
            (20, "Test Case 5: top_k=20"),
        ]
        
        for top_k, label in show_progress(test_cases, desc="Processing parallel test cases"):
            report_file.write(f"\n{label}\n")
            report_file.write("-" * 40 + "\n")
            try:
                with ExecutionTimer(f"Running {label}"):
                    run_ptf_algorithm_parallel_with_timing(
                        output_file=report_file,
                        file_path=dataset,
                        top_k=top_k,
                        num_workers=workers
                    )
            except FileNotFoundError as e:
                report_file.write(f"Error: {e}\n")
        
        report_file.close()
    print("Report written to: ptf_parallel_output.txt")


if __name__ == "__main__":
    main()
