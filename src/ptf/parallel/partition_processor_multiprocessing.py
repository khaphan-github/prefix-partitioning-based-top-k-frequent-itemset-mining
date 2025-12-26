"""
Multiprocessing Partition Processor for PTF Algorithm

Implements partition-level parallelization using ProcessPoolExecutor.
Each worker process processes a single prefix-based partition independently,
bypassing Python's GIL for true parallel execution on multiple CPU cores.

Key Design:
- True parallel execution (bypasses GIL)
- Each process has independent memory space
- Results are pickled and sent back to main process
- Best for CPU-bound tasks
"""

import os
import concurrent.futures
from typing import Tuple, List, Dict
from ptf.min_heap import MinHeapTopK


def _process_partition_worker(work_item: dict, partition_class) -> Tuple[dict, int]:
    """
    Worker function that runs in a separate process.
    
    This function must be at module level for pickling to work with multiprocessing.
    
    Args:
        work_item: Dict containing partition data
        partition_class: Partition processor class (must be picklable)
    
    Returns:
        Tuple of (itemsets_dict, local_rmsup)
    """
    # Create local min-heap for this process
    local_heap = MinHeapTopK(work_item['top_k'])
    
    # Insert initial itemsets
    for support, itemset in work_item['initial_itemsets']:
        local_heap.insert(support=support, itemset=itemset)
    
    # Execute partition processing
    result = partition_class.execute(
        partition_item=work_item['partition_item'],
        promising_items=work_item['promising_items'],
        partition_data=work_item['partition_data'],
        min_heap=local_heap,
        rmsup=work_item['initial_rmsup']
    )
    
    # Handle both 2-tuple and 3-tuple returns
    if len(result) == 3:
        local_mh, local_rmsup, _ = result
    else:
        local_mh, local_rmsup = result
    
    # Convert heap to dict for pickling (more efficient than heap object)
    itemsets_dict = {tuple(itemset): support for support, itemset in local_mh.get_all()}
    
    return itemsets_dict, local_rmsup


class MultiprocessingPartitionProcessor:
    """
    Manages parallel processing of prefix-based partitions using ProcessPoolExecutor.
    
    Each worker process:
    1. Gets a partition and its data
    2. Creates local copy of MH
    3. Calls partition_processor.execute() (ProcessSglPartition)
    4. Returns (itemsets_dict, local_rmsup)
    
    Main process:
    1. Submits all work items to process pool
    2. Collects results as processes complete
    3. Merges all results into global MH
    """
    
    def __init__(self, num_workers: int = None, partition_class=None):
        """
        Initialize multiprocessing partition processor.
        
        Args:
            num_workers: Number of worker processes. 
                        Default: os.cpu_count() (auto-detect cores)
            partition_class: Partition processor class 
                           (SglPartition, SglPartitionHybrid, or SglPartitionHybridCandidatePruning)
        """
        self.num_workers = num_workers or os.cpu_count() or 4
        self.partition_class = partition_class
        self.process_pool = concurrent.futures.ProcessPoolExecutor(
            max_workers=self.num_workers
        )
    
    def process_partitions(
        self,
        partitions: List[int],
        promising_arr: Dict[int, List[int]],
        partitioner,  # PrefixPartitioning object
        initial_min_heap: MinHeapTopK,
        initial_rmsup: int,
        top_k: int
    ) -> Tuple[MinHeapTopK, int]:
        """
        Process multiple partitions in parallel using multiprocessing.
        
        Algorithm:
        1. Build work items from partitions (skip unpromising)
        2. Submit each work item to process pool
        3. Collect results from processes as they complete
        4. Merge all results into global min-heap
        5. Return merged (min_heap, rmsup)
        
        Args:
            partitions: List of partition items to process
            promising_arr: Dict mapping partition item -> list of promising items (AR_i)
            partitioner: PrefixPartitioning object (contains partition_data)
            initial_min_heap: Starting min-heap (result of initialize_mh_and_rmsup)
            initial_rmsup: Starting rmsup value
            top_k: k value for min-heap (keep top-k itemsets)
        
        Returns:
            Tuple of (merged_min_heap, final_rmsup)
        """
        # Get initial itemsets for workers
        initial_itemsets = initial_min_heap.get_all()
        
        # Step 1: Build work items from valid partitions
        work_items = []
        for partition_item in partitions:
            # Skip if partition has no promising items
            promising_items = promising_arr.get(partition_item, [])
            if len(promising_items) <= 2:
                continue
            
            # Get partition data from partitioner
            partition_data = partitioner.prefix_partitions.get(partition_item, [])
            if not partition_data:
                continue
            
            work_item = {
                'partition_item': partition_item,
                'promising_items': promising_items,
                'partition_data': partition_data,
                'initial_rmsup': initial_rmsup,
                'top_k': top_k,
                'initial_itemsets': initial_itemsets
            }
            work_items.append(work_item)
        
        if not work_items:
            # No partitions to process, return initial state
            return initial_min_heap, initial_rmsup
        
        # Step 2: Submit work items to process pool
        futures = []
        for work_item in work_items:
            future = self.process_pool.submit(
                _process_partition_worker,
                work_item,
                self.partition_class
            )
            futures.append(future)
        
        # Step 3: Collect results from processes
        local_results = []  # List of (itemsets_dict, local_rmsup) tuples
        for future in concurrent.futures.as_completed(futures):
            try:
                itemsets_dict, local_rmsup = future.result()
                local_results.append((itemsets_dict, local_rmsup))
            except Exception as e:
                # Log error but continue with other results
                print(f"Worker process error: {e}")
                raise
        
        # Step 4: Merge all local results
        merged_mh, final_rmsup = self._merge_results(
            local_results,
            initial_min_heap,
            top_k
        )
        
        return merged_mh, final_rmsup
    
    def _merge_results(
        self,
        local_results: List[Tuple[dict, int]],
        initial_min_heap: MinHeapTopK,
        top_k: int
    ) -> Tuple[MinHeapTopK, int]:
        """
        Merge all local results into a single global min-heap.
        
        Algorithm:
        1. Create new empty top-k min-heap
        2. For each result from processes:
           - Insert all itemsets into merged heap
        3. MinHeapTopK maintains top-k invariant automatically
        4. Return merged heap
        
        Args:
            local_results: List of (itemsets_dict, local_rmsup) from processes
            initial_min_heap: Original MH before parallelization
            top_k: k value for final min-heap
        
        Returns:
            Tuple of (merged_min_heap, final_rmsup)
        """
        merged_heap = MinHeapTopK(top_k)
        
        # Insert all itemsets from local results (processes)
        for itemsets_dict, _ in local_results:
            for itemset, support in itemsets_dict.items():
                merged_heap.insert(support=support, itemset=itemset)
        
        # If no local results, use initial heap as fallback
        if not merged_heap.get_all() and initial_min_heap.get_all():
            for support, itemset in initial_min_heap.get_all():
                merged_heap.insert(support=support, itemset=itemset)
        
        # Get final rmsup (minimum support in top-k)
        final_rmsup = merged_heap.min_support()
        
        return merged_heap, final_rmsup
    
    def shutdown(self):
        """
        Shutdown the process pool and wait for all processes to complete.
        
        Should be called after process_partitions() completes to cleanup resources.
        """
        self.process_pool.shutdown(wait=True)
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup process pool"""
        self.shutdown()
        return False