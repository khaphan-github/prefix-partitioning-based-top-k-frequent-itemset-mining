"""
Utility for analyzing execution metrics saved as JSON files.
Can be used with pandas DataFrames.
"""

import json
from pathlib import Path
from typing import List, Dict, Union


class MetricsAnalyzer:
    """Analyze and compare execution metrics from JSON files."""
    
    @staticmethod
    def load_metrics(file_path: str) -> Union[Dict, List[Dict]]:
        """
        Load metrics from JSON file.
        
        Args:
            file_path: Path to JSON metrics file
            
        Returns:
            Dictionary or list of dictionaries with metrics
        """
        with open(file_path, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def to_pandas_dataframe(metrics_list: List[Dict]):
        """
        Convert metrics list to pandas DataFrame.
        
        Args:
            metrics_list: List of metrics dictionaries or single dictionary
            
        Returns:
            pandas DataFrame (requires pandas to be installed)
        """
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas is required for this functionality. Install with: pip install pandas")
        
        # Normalize to list
        if isinstance(metrics_list, dict):
            metrics_list = [metrics_list]
        
        # Flatten nested dictionaries
        flattened = []
        for metric in metrics_list:
            flat = {
                'timestamp': metric.get('timestamp'),
                'algorithm': metric.get('algorithm'),
                'execution_time_ms': metric.get('metrics', {}).get('execution_time_ms'),
                'memory_used_kb': metric.get('metrics', {}).get('memory_used_kb'),
            }
            
            # Add parameters if present
            if 'parameters' in metric:
                flat.update({f"param_{k}": v for k, v in metric['parameters'].items()})
            
            # Add results if present
            if 'results' in metric:
                flat.update({f"result_{k}": v for k, v in metric['results'].items()})
            
            flattened.append(flat)
        
        return pd.DataFrame(flattened)
    
    @staticmethod
    def compare_runs(
        base_metrics: Dict,
        comparison_metrics: Dict
    ) -> Dict:
        """
        Compare two metrics dictionaries.
        
        Args:
            base_metrics: First metrics dictionary
            comparison_metrics: Second metrics dictionary
            
        Returns:
            Dictionary with comparison results
        """
        base_time = base_metrics.get('metrics', {}).get('execution_time_ms', 0)
        comp_time = comparison_metrics.get('metrics', {}).get('execution_time_ms', 0)
        
        base_mem = base_metrics.get('metrics', {}).get('memory_used_kb', 0)
        comp_mem = comparison_metrics.get('metrics', {}).get('memory_used_kb', 0)
        
        time_diff = comp_time - base_time
        time_pct = (time_diff / base_time * 100) if base_time != 0 else 0
        
        mem_diff = comp_mem - base_mem
        mem_pct = (mem_diff / base_mem * 100) if base_mem != 0 else 0
        
        return {
            'base_execution_time_ms': base_time,
            'comparison_execution_time_ms': comp_time,
            'time_difference_ms': time_diff,
            'time_improvement_percent': time_pct,
            'base_memory_kb': base_mem,
            'comparison_memory_kb': comp_mem,
            'memory_difference_kb': mem_diff,
            'memory_improvement_percent': mem_pct,
            'speedup': base_time / comp_time if comp_time != 0 else 0
        }
    
    @staticmethod
    def print_comparison(comparison: Dict):
        """Pretty print comparison results."""
        print("Metrics Comparison Report")
        print("=" * 60)
        print(f"Execution Time:")
        print(f"  Base:        {comparison['base_execution_time_ms']:.2f} ms")
        print(f"  Comparison:  {comparison['comparison_execution_time_ms']:.2f} ms")
        print(f"  Difference:  {comparison['time_difference_ms']:+.2f} ms")
        print(f"  Change:      {comparison['time_improvement_percent']:+.1f}%")
        if comparison['time_improvement_percent'] < 0:
            print(f"  Speedup:     {comparison['speedup']:.2f}x")
        print()
        print(f"Memory Usage:")
        print(f"  Base:        {comparison['base_memory_kb']:.2f} KB")
        print(f"  Comparison:  {comparison['comparison_memory_kb']:.2f} KB")
        print(f"  Difference:  {comparison['memory_difference_kb']:+.2f} KB")
        print(f"  Change:      {comparison['memory_improvement_percent']:+.1f}%")
