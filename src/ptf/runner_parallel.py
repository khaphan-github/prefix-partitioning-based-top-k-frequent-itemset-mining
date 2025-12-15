"""
Parallel Runner Module for PTF Algorithm

Provides simplified interface for running the parallel PTF algorithm variant.
All functions here use PrefixPartitioningbasedTopKAlgorithmParallel internally.
"""

import time
from typing import Optional
from ptf.hybrid_vertical_storage.sgl_partition_hybrid_candidate_pruning import SglPartitionHybridCandidatePruning
from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.algorithm_parallel import PrefixPartitioningbasedTopKAlgorithmParallel
from ptf.utils import write_output, track_execution, MetricsReporter


def run_ptf_algorithm_parallel(
    file_path: str,
    top_k: int = 8,
    output_file=None,
    num_workers: int = None
):
    """
    Run PTF algorithm with partition-level parallelization.

    This version uses multiple threads to process prefix-based partitions
    in parallel, providing significant speedup on multi-core systems.

    Algorithm:
    1. SEQUENTIAL: Initialize MH, rmsup, and promising item arrays
    2. PARALLEL: Process partitions using thread pool
    3. SEQUENTIAL: Merge results and output

    Args:
        file_path: Path to transaction database file
        top_k: Number of top-k itemsets to find
        output_file: Optional file object to write results to
        num_workers: Number of threads. Default: os.cpu_count() (auto-detect)
    
    Example:
        >>> run_ptf_algorithm_parallel("data/transactions.txt", top_k=8)
        >>> run_ptf_algorithm_parallel("data/transactions.txt", top_k=8, num_workers=4)
    """
    with track_execution() as metrics:
        # Phase 1: Sequential initialization
        db = TransactionDB(file_path)
        partitioner = PrefixPartitioning(db)
        co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)

        # Main algorithm - use PARALLEL variant
        ptf = PrefixPartitioningbasedTopKAlgorithmParallel(
            k=top_k,
            partitionClass=SglPartitionHybridCandidatePruning,
            num_workers=num_workers
        )
        min_heap, rmsup = ptf.initialize_mh_and_rmsup(
            co_occurrence_numbers.full_co_occurrence_list)

        write_output(f"Initial rmsup: {rmsup}", output_file)
        write_output(f"Initial MH size: {len(min_heap.heap)}", output_file)

        promissing_arr = ptf.build_promissing_item_arrays(
            min_heap=min_heap,
            all_items=db.all_items,
            con_map=co_occurrence_numbers.con_map,
            rmsup=rmsup
        )

        # Phase 2: Parallel partition processing (overridden filter_partitions)
        min_heap, rmsup = ptf.filter_partitions(
            promissing_arr=promissing_arr,
            partitions=db.all_items,
            min_heap=min_heap,
            con_map=co_occurrence_numbers.con_map,
            rmsup=rmsup,
            partitioner=partitioner
        )

        # Phase 3: Output results
        final_results = min_heap.get_all()
        final_results.sort(key=lambda x: (-x[0], x[1]))

        write_output(
            f"\nTotal itemsets found: {len(final_results)}", output_file)
        write_output(f"Final rmsup: {min_heap.min_support()}\n", output_file)

    # Generate execution report (after metrics are finalized)
    write_output(
        f"Execution time: {metrics.execution_time_ms:.2f} ms", output_file)
    write_output(f"Memory used: {metrics.memory_used_kb:.2f} KB", output_file)

    for rank, (support, itemset) in enumerate(final_results, 1):
        itemset_str = "{" + ", ".join(map(str, sorted(itemset))) + "}"
        write_output(
            f"{rank}. {itemset_str:20} => Support: {support}", output_file)


def run_ptf_algorithm_parallel_with_timing(
    file_path: str,
    top_k: int = 8,
    output_file=None,
    num_workers: int = None,
    metrics_json: Optional[str] = None
) -> float:
    """
    Run parallel PTF algorithm and measure execution time.

    Args:
        file_path: Path to transaction database file
        top_k: Number of top-k itemsets to find
        output_file: Optional file object to write results to
        num_workers: Number of threads. Default: os.cpu_count()
        metrics_json: Optional JSON file path to save metrics report

    Returns:
        Execution time in seconds
    
    Example:
        >>> exec_time = run_ptf_algorithm_parallel_with_timing("data.txt", top_k=8, num_workers=4)
        >>> print(f"Execution time: {exec_time:.4f}s")
    """
    with track_execution() as metrics:
        run_ptf_algorithm_parallel(file_path, top_k, output_file, num_workers)

    execution_time = metrics.execution_time
    write_output(f"Execution time: {execution_time:.4f} seconds", output_file)

    if metrics_json:
        MetricsReporter.save_metrics(
            metrics,
            metrics_json,
            algorithm="PTF-Parallel",
            top_k=top_k,
            num_workers=num_workers
        )

    return execution_time
