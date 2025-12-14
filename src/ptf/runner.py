import time
from typing import Optional
from ptf.hybrid_vertical_storage.sgl_partition_hybrid_candidate_pruning import SglPartitionHybridCandidatePruning
from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm
from ptf.utils import write_output, track_execution


def run_ptf_algorithm(file_path: str, top_k: int = 8, output_file=None):
    """
    Helper function to run PTF algorithm with given parameters.
    Writes output to file if output_file is provided.
    
    Args:
        file_path: Path to transaction database file
        top_k: Number of top-k itemsets to find
        output_file: Optional file object to write results to
    """
    with track_execution() as metrics:
        # Read transaction database
        db = TransactionDB(file_path)
        
        partitioner = PrefixPartitioning(db)
        co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)

        # Main algorithm
        ptf = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartitionHybridCandidatePruning)
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

        min_heap, rmsup = ptf.filter_partitions(
            promissing_arr=promissing_arr,
            partitions=db.all_items,
            min_heap=min_heap,
            con_map=co_occurrence_numbers.con_map,
            rmsup=rmsup,
            partitioner=partitioner
        )

        # Print final results
        final_results = min_heap.get_all()
        # Sort by support descending
        final_results.sort(key=lambda x: (-x[0], x[1]))

        write_output(f"\nTotal itemsets found: {len(final_results)}", output_file)
        write_output(f"Final rmsup: {min_heap.min_support()}\n", output_file)

        for rank, (support, itemset) in enumerate(final_results, 1):
            itemset_str = "{" + ", ".join(map(str, sorted(itemset))) + "}"
            write_output(f"{rank}. {itemset_str:20} => Support: {support}", output_file)
    
    # Generate execution report
    write_output(f"Execution time: {metrics.execution_time:.4f} seconds", output_file)
    write_output(f"Memory used: {metrics.memory_used:.2f} MB", output_file)


def run_ptf_algorithm_with_timing(file_path: str, top_k: int = 8, output_file=None) -> float:
    """
    Run PTF algorithm and measure execution time.
    
    Args:
        file_path: Path to transaction database file
        top_k: Number of top-k itemsets to find
        output_file: Optional file object to write results to
        
    Returns:
        Execution time in seconds
    """
    start_time = time.time()
    run_ptf_algorithm(file_path, top_k, output_file)
    end_time = time.time()
    
    execution_time = end_time - start_time
    write_output(f"Execution time: {execution_time:.4f} seconds", output_file)
    
    return execution_time
