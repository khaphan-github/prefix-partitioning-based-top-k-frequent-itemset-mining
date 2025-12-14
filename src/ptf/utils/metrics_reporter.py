"""
Metrics reporter module for saving execution metrics to JSON files.
"""

import json
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path


class MetricsReporter:
    """Generate and save execution metrics reports in JSON format."""
    
    @staticmethod
    def save_metrics(
        metrics,
        output_path: str,
        algorithm: str = "PTF",
        top_k: Optional[int] = None,
        total_itemsets: Optional[int] = None,
        final_rmsup: Optional[int] = None,
        **kwargs
    ):
        """
        Save execution metrics to JSON file.
        
        Args:
            metrics: ExecutionMetrics object
            output_path: Path to save JSON report
            algorithm: Algorithm name (e.g., "PTF", "PTF-Parallel")
            top_k: Top-k parameter
            total_itemsets: Total itemsets found
            final_rmsup: Final rmsup value
            **kwargs: Additional parameters (e.g., num_workers, dataset_name)
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "algorithm": algorithm,
            "metrics": {
                "execution_time_ms": metrics.execution_time_ms,
                "memory_used_kb": metrics.memory_used_kb
            }
        }
        
        # Build parameters dict
        params = {}
        if top_k is not None:
            params["top_k"] = top_k
        # Add any additional kwargs to parameters
        params.update({k: v for k, v in kwargs.items() if v is not None})
        
        if params:
            report["parameters"] = params
        
        if total_itemsets is not None or final_rmsup is not None:
            report["results"] = {}
            if total_itemsets is not None:
                report["results"]["total_itemsets"] = total_itemsets
            if final_rmsup is not None:
                report["results"]["final_rmsup"] = final_rmsup
        
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2)
    
    @staticmethod
    def append_metrics(
        metrics,
        output_path: str,
        algorithm: str = "PTF",
        **kwargs
    ):
        """
        Append metrics to existing JSON report (for multiple runs).
        
        Args:
            metrics: ExecutionMetrics object
            output_path: Path to JSON report
            algorithm: Algorithm name
            **kwargs: Additional metrics to save
        """
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Load existing data or create new list
        if Path(output_path).exists():
            with open(output_path, 'r') as f:
                reports = json.load(f)
                if not isinstance(reports, list):
                    reports = [reports]
        else:
            reports = []
        
        # Create new report
        report = {
            "timestamp": datetime.now().isoformat(),
            "algorithm": algorithm,
            "metrics": {
                "execution_time_ms": metrics.execution_time_ms,
                "memory_used_kb": metrics.memory_used_kb
            }
        }
        
        # Add any additional kwargs
        for key, value in kwargs.items():
            if value is not None:
                report[key] = value
        
        reports.append(report)
        
        with open(output_path, 'w') as f:
            json.dump(reports, f, indent=2)
