"""
Main entry point for PTF algorithm with JSON configuration.

Usage:
    python main_with_json_config.py config.json

Config JSON format:
{
    "top_k": 8,
    "parallel": false,
    "input_dataset_path": "path/to/transactions.txt",
    "output_report": "reports/dataset-a/",
    "num_workers": 4,  // optional, only used if parallel=true
    "save_metrics": true  // optional, default: true
}

Notes:
- output_report should be a FOLDER path (with trailing slash)
- Reports are auto-named: {algorithm}_report_{timestamp}.txt
- Metrics are auto-named: {algorithm}_metrics_{timestamp}.json
"""

import json
import sys
from pathlib import Path
from typing import Dict, Optional
import argparse
from datetime import datetime


def validate_config(config: Dict) -> tuple[bool, str]:
    """
    Validate configuration JSON.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Check required fields
    required_fields = ['top_k', 'input_dataset_path', 'output_report']
    for field in required_fields:
        if field not in config:
            return False, f"Missing required field: {field}"
    
    # Validate top_k
    try:
        top_k = int(config['top_k'])
        if top_k <= 0:
            return False, "top_k must be a positive integer"
    except (ValueError, TypeError):
        return False, "top_k must be an integer"
    
    # Validate input_dataset_path exists
    input_path = Path(config['input_dataset_path'])
    if not input_path.exists():
        return False, f"Input dataset not found: {config['input_dataset_path']}"
    
    # Validate num_workers if provided
    if 'num_workers' in config:
        try:
            num_workers = int(config['num_workers'])
            if num_workers <= 0:
                return False, "num_workers must be a positive integer"
        except (ValueError, TypeError):
            return False, "num_workers must be an integer"
    
    return True, ""


def load_config(config_path: str) -> Optional[Dict]:
    """
    Load configuration from JSON file.
    
    Args:
        config_path: Path to JSON config file
        
    Returns:
        Configuration dictionary or None if error
    """
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Config file not found: {config_path}")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in config file: {e}")
        return None


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
                    print(f"Running Multiprocessing PTF with {num_workers or 'auto'} workers...")
                    exec_time = run_ptf_algorithm_multiprocessing_with_timing(
                        file_path=input_path,
                        top_k=top_k,
                        output_file=output_file,
                        num_workers=num_workers,
                        metrics_json=metrics_file
                    )
                    print(f"Multiprocessing execution completed in {exec_time:.4f} seconds")
                else:
                    print(f"Running Parallel PTF (Threading) with {num_workers or 'auto'} workers...")
                    exec_time = run_ptf_algorithm_parallel_with_timing(
                        file_path=input_path,
                        top_k=top_k,
                        output_file=output_file,
                        num_workers=num_workers,
                        metrics_json=metrics_file
                    )
                    print(f"Parallel execution completed in {exec_time:.4f} seconds")
            else:
                print("Running Sequential PTF...")
                exec_time = run_ptf_algorithm_with_timing(
                    file_path=input_path,
                    top_k=top_k,
                    output_file=output_file,
                    metrics_json=metrics_file
                )
                print(f"Sequential execution completed in {exec_time:.4f} seconds")
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


def main(config_path: str):
    """
    Main function to run PTF algorithm from JSON config.
    
    Args:
        config_path: Path to JSON configuration file
    """
    print(f"Loading configuration from: {config_path}\n")
    
    # Load config
    config = load_config(config_path)
    if config is None:
        return 1
    
    # Validate config
    is_valid, error_msg = validate_config(config)
    if not is_valid:
        print(f"Error: {error_msg}")
        return 1
    
    print("Configuration loaded successfully:")
    print(json.dumps(config, indent=2))
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
        description="Run PTF algorithm with JSON configuration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main_with_json_config.py config.json
  
Config JSON format:
  {
    "top_k": 8,
    "parallel": false,
    "input_dataset_path": "data/transactions.txt",
    "output_report": "reports/output.txt",
    "num_workers": 4,
    "save_metrics": true
  }
        """
    )
    
    parser.add_argument(
        'config',
        help='Path to JSON configuration file'
    )
    
    parser.add_argument(
        '-o', '--output',
        help='Override output report folder from config',
        default=None
    )
    
    parser.add_argument(
        '-k', '--top-k',
        type=int,
        help='Override top_k value from config',
        default=None
    )
    
    parser.add_argument(
        '-p', '--parallel',
        action='store_true',
        help='Force parallel execution'
    )
    
    parser.add_argument(
        '-w', '--workers',
        type=int,
        help='Number of workers for parallel execution',
        default=None
    )
    
    args = parser.parse_args()
    
    # Load config
    config = load_config(args.config)
    if config is None:
        sys.exit(1)
    
    # Override with command-line arguments
    if args.output:
        config['output_report'] = args.output
    if args.top_k:
        config['top_k'] = args.top_k
    if args.parallel:
        config['parallel'] = True
    if args.workers:
        config['num_workers'] = args.workers
    
    # Run
    exit_code = main(args.config)
    sys.exit(exit_code)
