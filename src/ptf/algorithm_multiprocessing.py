"""
Multiprocessing version of Prefix Partitioning-based Top-K Frequent Itemset Mining Algorithm

This module provides a multiprocessing variant that processes prefix-based partitions
in parallel using a process pool, bypassing Python's GIL for true parallel execution.

Key Difference from algorithm.py:
- filter_partitions_multiprocessing(): Uses MultiprocessingPartitionProcessor for partition-level parallelism
- All other methods are identical to the sequential version
"""

from typing import List, Tuple, Dict, Set
from ptf.min_heap import MinHeapTopK
from ptf.sgl_partition import SglPartition
from ptf.hybrid_vertical_storage.sgl_partition_hybrid import SglPartitionHybrid
from ptf.hybrid_vertical_storage.sgl_partition_hybrid_candidate_pruning import SglPartitionHybridCandidatePruning
from ptf.parallel.partition_processor_multiprocessing import MultiprocessingPartitionProcessor
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm


class PrefixPartitioningbasedTopKAlgorithmMultiprocessing(PrefixPartitioningbasedTopKAlgorithm):
    """
    Multiprocessing variant of PTF algorithm with partition-level parallelization.
    
    Inherits all sequential methods from PrefixPartitioningbasedTopKAlgorithm
    and overrides filter_partitions() with a multiprocessing version.
    
    All other methods (initialize_mh_and_rmsup, build_promissing_item_arrays, etc.)
    remain identical to the parent class.
    
    Key Benefits:
    - True parallel execution (bypasses Python GIL)
    - Better performance for CPU-bound tasks
    - Utilizes multiple CPU cores effectively
    """
    
    def __init__(self, k: int, partitionClass, num_workers: int = None):
        """
        Initialize multiprocessing PTF algorithm.
        
        Args:
            k: Number of top-k frequent itemsets to find
            partitionClass: Partition processor class
            num_workers: Number of worker processes. Default: os.cpu_count()
        """
        super().__init__(k, partitionClass)
        self.num_workers = num_workers
    
    def filter_partitions(
        self,
        promissing_arr: dict,
        partitions: List[int],
        min_heap: MinHeapTopK,
        con_map: dict,
        rmsup: int,
        partitioner=None
    ):
        """
        Multiprocessing version of filter_partitions using partition-level parallelism.
        
        Algorithm:
        1. SEQUENTIAL: Filter unpromising partitions (Algorithm 1, lines 12-19)
           - Skip partitions with |AR_i| <= 2
           - Skip partitions with low support
        
        2. PARALLEL: Process remaining partitions using process pool
           - Each worker process processes one partition independently
           - Each process has its own memory space (no GIL)
           - Returns (itemsets_dict, local_rmsup)
        
        3. MERGE: Combine results from all workers
           - Merge all results into global MH
           - Recalculate global rmsup
        
        Args:
            promissing_arr: Dict mapping partition -> promising items (AR_i)
            partitions: List of all partition items
            min_heap: Current top-k itemsets
            con_map: Co-occurrence map for pruning
            rmsup: Current running minimum support threshold
            partitioner: PrefixPartitioning object (contains partition_data)
        
        Returns:
            Tuple of (updated_min_heap, updated_rmsup)
        """
        # Step 1: SEQUENTIAL pruning (Algorithm 1, lines 12-19)
        for partition in partitions:
            # Remove unpromising items from AR_i
            for promissing_item in promissing_arr[partition]:
                partition_support = con_map.get((partition,), 0)
                if promissing_item == partition and partition_support <= rmsup:
                    break
                
                if promissing_item > partition:
                    pair_key = (partition, promissing_item)
                    pair_support = con_map.get(pair_key, 0)
                    if pair_support <= rmsup:
                        promissing_arr[partition].remove(promissing_item)
        
        # Step 2: PARALLEL partition processing with multiprocessing
        if partitioner and hasattr(partitioner, 'prefix_partitions'):
            processor = MultiprocessingPartitionProcessor(
                num_workers=self.num_workers,
                partition_class=self.partition_processor
            )
            try:
                min_heap, rmsup = processor.process_partitions(
                    partitions=partitions,
                    promising_arr=promissing_arr,
                    partitioner=partitioner,
                    initial_min_heap=min_heap,
                    initial_rmsup=rmsup,
                    top_k=self.top_k
                )
            finally:
                processor.shutdown()
        
        return min_heap, rmsup