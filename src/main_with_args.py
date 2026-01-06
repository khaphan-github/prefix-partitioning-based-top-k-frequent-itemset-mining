import sys
from pathlib import Path
from typing import Dict
import argparse
from datetime import datetime


def run_algorithm(config: Dict) -> bool:
    try:
        # Import here to avoid import errors if ptf module not available
        from ptf.runner import run_ptf_algorithm_with_timing
        from ptf.runner_parallel import run_ptf_algorithm_parallel_with_timing
        from ptf.runner_multiprocessing import run_ptf_algorithm_multiprocessing_with_timing

        # Extract configuration
        top_k = int(config['top_k'])
        input_path = config['input_dataset_path']
        output_base = config['output_report']
        num_workers = config.get('num_workers', None)
        save_metrics = config.get('save_metrics', True)
        runner_name = config.get('runner_name', None)

        if not runner_name:
            raise ValueError("runner_name must be specified in config")

        output_dir = Path(output_base)
        output_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"{runner_name}_report_{timestamp}.txt"
        output_report = str(output_dir / report_filename)

        # Prepare metrics file path
        metrics_file = None
        if save_metrics:
            metrics_filename = f"{runner_name}_metrics_{timestamp}.json"
            metrics_file = str(output_dir / metrics_filename)

        # Write header to output file
        with open(output_report, 'w') as f:
            f.write("=" * 70 + "\n")
            f.write(f"PTF Algorithm Report\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Configuration:\n")
            f.write(f"  Top-k: {top_k}\n")
            f.write(f"  Input Dataset: {input_path}\n")
            f.write(f"\n")
            f.close()

        # Run appropriate algorithm
        output_file = open(output_report, 'a')
        if runner_name == 'run_ptf_algorithm_multiprocessing_with_timing':
            run_ptf_algorithm_multiprocessing_with_timing(
                file_path=input_path,
                top_k=top_k,
                output_file=output_file,
                num_workers=num_workers,
                metrics_json=metrics_file
            )

        elif runner_name == 'run_ptf_algorithm_parallel_with_timing':
            run_ptf_algorithm_parallel_with_timing(
                file_path=input_path,
                top_k=top_k,
                output_file=output_file,
                num_workers=num_workers,
                metrics_json=metrics_file
            )
        # Defulat sequential runner
        elif runner_name == 'sequential':
            run_ptf_algorithm_with_timing(
                file_path=input_path,
                top_k=top_k,
                output_file=output_file,
                metrics_json=metrics_file
            )
        # Run concurent co-occurrent version
        elif runner_name == 'parallel_co_occurrent':
            from ptf.runner_parallel_co_occurrent import run_ptf_algorithm_with_timing_co_occurent
            run_ptf_algorithm_with_timing_co_occurent(
                file_path=input_path,
                top_k=top_k,
                output_file=output_file,
                metrics_json=metrics_file
            )
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
    success = run_algorithm(config)
    print('Done')


if __name__ == "__main__":
    # Define dataset configurations
    REPORT_FOL = './benchmark/reports_06_01_v1'
    datasets = [
        {
            'name': 'pumsb',
            'file': './data/data_set/pumsb.txt',
            'output_dir': f'{REPORT_FOL}/pumsb',
            'top_k_values': [1000, 3000, 5000, 7000, 10000]
        },
        {
            'name': 'OnlineRetailZZ',
            'file': './data/data_set/OnlineRetailZZ.txt',
            'output_dir': f'{REPORT_FOL}/OnlineRetailZZ',
            'top_k_values': [1000, 3000, 5000, 7000, 10000]
        },
        {
            'name': 'chainstoreFIM',
            'file': './data/data_set/chainstoreFIM.txt',
            'output_dir': f'{REPORT_FOL}/chainstoreFIM',
            'top_k_values': [1000, 3000, 5000, 7000, 10000]
        }
    ]

    # Define runner configurations
    runners = [
        # Tuan tu
        'sequential',
        # XOn xong kha uopdate
        'parallel_co_occurrent'
    ]

    # Generate configurations from datasets and runners
    configs = []
    for dataset in datasets:
        for top_k in dataset['top_k_values']:
            for runner_name in runners:
                configs.append({
                    'top_k': top_k,
                    'input_dataset_path': dataset['file'],
                    'output_report': dataset['output_dir'] + f'_{runner_name}',
                    'runner_name': runner_name,
                    'num_workers': None,
                    'save_metrics': True
                })

    # Run all configurations
    for config in configs:
        print(f"\n{'='*70}")
        print(
            f"Running with top-k={config['top_k']} on {config['input_dataset_path']}")
        print(f"{'='*70}")
        success = main(config)
        if not success:
            print(f"Failed to run configuration with top-k={config['top_k']}")

    print("\nAll configurations completed!")
    sys.exit(0)
