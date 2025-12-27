"""
Main entry point for PTF algorithm with command-line arguments.

Usage:
    python main_with_args.py --top-k 8 --input data/transactions.txt --output reports/output/ [--parallel] [--workers 4] [--save-metrics]

Arguments:
    --top-k K                   Top-k frequent itemsets (required)
    --input PATH                Input dataset path (required)
    --output PATH               Output report folder (required)
    --parallel                  Enable parallel execution (optional, default: sequential)
    --workers N                 Number of workers for parallel execution (optional)
    --multiprocessing          Use multiprocessing instead of threading (optional)
    --save-metrics             Save metrics to JSON file (optional, default: True)

Examples:
    python main_with_args.py --top-k 8 --input data/transactions.txt --output reports/
    python main_with_args.py --top-k 16 --input data/big_dataset.txt --output reports/ --parallel --workers 4
    python main_with_args.py --top-k 5 --input data/small.txt --output reports/ --parallel --multiprocessing --workers 8
"""

import sys
from pathlib import Path
from typing import Dict
import argparse
from datetime import datetime


def validate_args(args) -> tuple[bool, str]:
    """
    Validate command-line arguments.

    Args:
        args: Parsed arguments

    Returns:
        Tuple of (is_valid, error_message)
    """
    # Validate top_k
    if args.top_k <= 0:
        return False, "top_k must be a positive integer"

    # Validate input_dataset_path exists
    input_path = Path(args.input)
    if not input_path.exists():
        return False, f"Input dataset not found: {args.input}"

    # Validate num_workers if provided
    if args.workers and args.workers <= 0:
        return False, "workers must be a positive integer"

    # Validate that multiprocessing is only used with parallel
    if args.multiprocessing and not args.parallel:
        return False, "multiprocessing can only be used with --parallel flag"

    return True, ""


def build_config(args) -> Dict:
    """
    Build configuration dictionary from command-line arguments.

    Args:
        args: Parsed arguments

    Returns:
        Configuration dictionary
    """
    config = {
        'top_k': args.top_k,
        'input_dataset_path': args.input,
        'output_report': args.output,
        'parallel': args.parallel,
        'use_multiprocessing': args.multiprocessing,
        'num_workers': args.workers,
        'save_metrics': args.save_metrics
    }
    return config


def run_algorithm(config: Dict) -> bool:
    """
    Run PTF algorithm based on configuration.

    Args:
        config: Configuration dictionary

    Returns:
        True if successful, False otherwise
    """
    try:
        # Import here to avoid import errors if ptf module not available
        from ptf.runner import run_ptf_algorithm_with_timing
        from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing
        from ptf.runner_multiprocessing import run_ptf_algorithm_multiprocessing_with_timing

        # Extract configuration
        top_k = int(config['top_k'])
        input_path = config['input_dataset_path']
        output_base = config['output_report']
        parallel = config.get('parallel', False)
        num_workers = config.get('num_workers', None)
        use_multiprocessing = config.get('use_multiprocessing', False)
        save_metrics = config.get('save_metrics', True)

        # Handle output_report as folder path
        output_dir = Path(output_base)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate report filename based on algorithm type
        if parallel:
            if use_multiprocessing:
                algorithm_name = "multiprocessing"
            else:
                algorithm_name = "parallel"
        else:
            algorithm_name = "sequential"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"{algorithm_name}_report_{timestamp}.txt"
        output_report = str(output_dir / report_filename)

        # Prepare metrics file path
        metrics_file = None
        if save_metrics:
            metrics_filename = f"{algorithm_name}_metrics_{timestamp}.json"
            metrics_file = str(output_dir / metrics_filename)

        # Write header to output file
        with open(output_report, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write(f"PTF Algorithm Report\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Configuration:\n")
            if parallel:
                if use_multiprocessing:
                    f.write(f"  Algorithm: Multiprocessing PTF (True Parallel)\n")
                else:
                    f.write(f"  Algorithm: Parallel PTF (Threading)\n")
            else:
                f.write(f"  Algorithm: Sequential PTF\n")
            f.write(f"  Top-k: {top_k}\n")
            f.write(f"  Input Dataset: {input_path}\n")
            if parallel and num_workers:
                f.write(f"  Workers: {num_workers}\n")
            f.write(f"\n")

        # Run appropriate algorithm
        output_file = open(output_report, 'a')

        try:
            if parallel:
                if use_multiprocessing:
                    print(
                        f"Running Multiprocessing PTF with {num_workers or 'auto'} workers...")
                    exec_time = run_ptf_algorithm_multiprocessing_with_timing(
                        file_path=input_path,
                        top_k=top_k,
                        output_file=output_file,
                        num_workers=num_workers,
                        metrics_json=metrics_file
                    )
                    print(
                        f"Multiprocessing execution completed in {exec_time:.4f} seconds")
                else:
                    print(
                        f"Running Parallel PTF (Threading) with {num_workers or 'auto'} workers...")
                    exec_time = run_ptf_algorithm_parallel_with_timing(
                        file_path=input_path,
                        top_k=top_k,
                        output_file=output_file,
                        num_workers=num_workers,
                        metrics_json=metrics_file
                    )
                    print(
                        f"Parallel execution completed in {exec_time:.4f} seconds")
            else:
                print("Running Sequential PTF...")
                exec_time = run_ptf_algorithm_with_timing(
                    file_path=input_path,
                    top_k=top_k,
                    output_file=output_file,
                    metrics_json=metrics_file
                )
                print(
                    f"Sequential execution completed in {exec_time:.4f} seconds")
        finally:
            output_file.close()

        # Print summary
        print(f"\nResults saved to: {output_report}")
        if metrics_file:
            print(f"Metrics saved to: {metrics_file}")

        return True

    except ImportError as e:
        print(f"Error: Failed to import PTF modules: {e}")
        return False
    except Exception as e:
        print(f"Error during algorithm execution: {e}")
        import traceback
        traceback.print_exc()
        return False


def main(config: Dict):
    """
    Main function to run PTF algorithm from configuration.

    Args:
        config: Configuration dictionary
    """
    print("Configuration:")
    for key, value in config.items():
        print(f"  {key}: {value}")
    print()

    # Run algorithm
    success = run_algorithm(config)

    if success:
        print("\n✓ Algorithm execution completed successfully")
        return 0
    else:
        print("\n✗ Algorithm execution failed")
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run PTF algorithm with command-line arguments",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main_with_args.py --top-k 8 --input data/transactions.txt --output reports/
  python main_with_args.py --top-k 16 --input data/big_dataset.txt --output reports/ --parallel --workers 4
  python main_with_args.py --top-k 5 --input data/small.txt --output reports/ --parallel --multiprocessing --workers 8
        """
    )

    parser.add_argument(
        '--top-k',
        type=int,
        required=True,
        help='Top-k frequent itemsets to find (required)'
    )

    parser.add_argument(
        '--input',
        required=True,
        help='Path to input dataset file (required)'
    )

    parser.add_argument(
        '--output',
        required=True,
        help='Output report folder path (required)'
    )

    parser.add_argument(
        '--parallel',
        action='store_true',
        help='Enable parallel execution (threading by default)'
    )

    parser.add_argument(
        '--multiprocessing',
        action='store_true',
        help='Use multiprocessing instead of threading (only with --parallel)'
    )

    parser.add_argument(
        '--workers',
        type=int,
        default=None,
        help='Number of workers for parallel execution (optional, auto if not specified)'
    )

    parser.add_argument(
        '--save-metrics',
        action='store_true',
        default=True,
        help='Save metrics to JSON file (enabled by default)'
    )

    parser.add_argument(
        '--no-save-metrics',
        action='store_false',
        dest='save_metrics',
        help='Disable saving metrics'
    )

    args = parser.parse_args()

    # Validate arguments
    is_valid, error_msg = validate_args(args)
    if not is_valid:
        print(f"Error: {error_msg}")
        sys.exit(1)

    # Build config from arguments
    config = build_config(args)

    # Run
    exit_code = main(config)
    sys.exit(exit_code)
