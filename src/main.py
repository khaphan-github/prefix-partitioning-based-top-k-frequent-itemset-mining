from ptf.utils import ExecutionTimer, measure_execution_time, show_progress
from ptf.runner import run_ptf_algorithm


if __name__ == "__main__":
    with ExecutionTimer("PTF Algorithm - All Test Cases"):
        report_file = open("ptf_algorithm_report.txt", "w")

        test_cases = [
            (8, "Test Case 1: top_k=8"),
        ]

        for top_k, label in show_progress(test_cases, desc="Processing test cases"):
            report_file.write(f"\n{label}\n")
            report_file.write("-" * 40 + "\n")
            try:
                with ExecutionTimer(f"Running {label}"):
                    run_ptf_algorithm("data/data_set/sample.txt",
                                      top_k=top_k, output_file=report_file)
            except FileNotFoundError as e:
                report_file.write(f"Error: {e}\n")

        report_file.close()
    print("Report written to: ptf_algorithm_report.txt")
