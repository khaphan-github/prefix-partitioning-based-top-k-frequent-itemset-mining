"""
Parallel Partition Processor for PTF Algorithm

Implements partition-level parallelization using thread pool.
Each worker thread processes a single prefix-based partition independently,
maintaining local copies of the min-heap. Results are merged after all 
partitions complete.

Key Design:
- Thread-safe by design (no shared mutable state during processing)
- Local MH buffers avoid lock contention
- Merge phase combines results from all workers
"""

import os
import concurrent.futures
from typing import Tuple, List, Dict
from ptf.min_heap import MinHeapTopK


class ParallelPartitionProcessor:
    """
    Manages parallel processing of prefix-based partitions using ThreadPoolExecutor.
    
    Each worker thread:
    1. Gets a partition and its data
    2. Creates local copy of MH
    3. Calls partition_processor.execute() (ProcessSglPartition)
    4. Returns (local_min_heap, local_rmsup)
    
    Main thread:
    1. Submits all work items to thread pool
    2. Collects results as threads complete
    3. Merges all local MH into global MH
    """
    
    def __init__(self, num_workers: int = None, partition_class=None):
        """
        Initialize parallel partition processor.
        
        Args:
            num_workers: Number of worker threads. 
                        Default: os.cpu_count() (auto-detect cores)
            partition_class: Partition processor class 
                           (SglPartition, SglPartitionHybrid, SglPartitionHybridCandidatePruning)
        """
        self.num_workers = num_workers or os.cpu_count() or 4
        self.partition_class = partition_class
        self.thread_pool = concurrent.futures.ThreadPoolExecutor(
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
        Process multiple partitions in parallel.
        
        Algorithm:
        1. Build work items from partitions (skip unpromising)
        2. Submit each work item to thread pool
        3. Collect results from workers as they complete
        4. Merge all local min-heaps into global min-heap
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
                'initial_min_heap_copy': self._copy_min_heap(initial_min_heap),
                'initial_rmsup': initial_rmsup
            }
            work_items.append(work_item)
        
        if not work_items:
            # No partitions to process, return initial state
            return initial_min_heap, initial_rmsup
        
        # Step 2: Submit work items to thread pool
        futures = []
        for work_item in work_items:
            future = self.thread_pool.submit(
                self._process_single_partition,
                work_item
            )
            futures.append(future)
        
        # Step 3: Collect results from workers
        local_results = []  # List of (local_MH, local_rmsup) tuples
        for future in concurrent.futures.as_completed(futures):
            try:
                local_mh, local_rmsup = future.result()
                local_results.append((local_mh, local_rmsup))
            except Exception as e:
                # Log error but continue with other results
                print(f"Worker thread error: {e}")
                raise
        
        # Step 4: Merge all local results
        merged_mh, final_rmsup = self._merge_results(
            local_results,
            initial_min_heap,
            top_k
        )
        
        return merged_mh, final_rmsup
    
    def _process_single_partition(self, work_item: dict) -> Tuple[MinHeapTopK, int]:
        """
        Process a single partition (runs on worker thread).
        
        This method is called by each worker thread independently.
        No shared state is accessed (thread-safe by design).
        
        Args:
            work_item: Dict with keys:
                - 'partition_item': The prefix item (int)
                - 'promising_items': List of promising items (AR_i)
                - 'partition_data': Raw transaction data for this partition
                - 'initial_min_heap_copy': Local copy of MH
                - 'initial_rmsup': Initial rmsup value
        
        Returns:
            Tuple of (updated_local_min_heap, updated_local_rmsup)
        """
        # Execute partition processing using the partition processor class
        # (SglPartition, SglPartitionHybrid, or SglPartitionHybridCandidatePruning)
        result = self.partition_class.execute(
            partition_item=work_item['partition_item'],
            promising_items=work_item['promising_items'],
            partition_data=work_item['partition_data'],
            min_heap=work_item['initial_min_heap_copy'],
            rmsup=work_item['initial_rmsup']
        )
        
        # Handle both 2-tuple and 3-tuple returns
        # SglPartitionHybridCandidatePruning returns (mh, rmsup, top2_set)
        if len(result) == 3:
            local_mh, local_rmsup, _ = result
        else:
            local_mh, local_rmsup = result
        
        return local_mh, local_rmsup
    
    def _merge_results(
        self,
        local_results: List[Tuple[MinHeapTopK, int]],
        initial_min_heap: MinHeapTopK,
        top_k: int
    ) -> Tuple[MinHeapTopK, int]:
        """
        Merge all local min-heaps into a single global min-heap.
        
        Algorithm:
        1. Create new empty top-k min-heap
        2. For each local heap from workers:
           - Insert all itemsets into merged heap
           (Each local heap already contains initial itemsets)
        3. MinHeapTopK maintains top-k invariant automatically
        4. Return merged heap
        
        Args:
            local_results: List of (local_MH, local_rmsup) from workers
            initial_min_heap: Original MH before parallelization (not reinserted to avoid duplicates)
            top_k: k value for final min-heap
        
        Returns:
            Tuple of (merged_min_heap, final_rmsup)
        """
        merged_heap = MinHeapTopK(top_k)
        
        # Insert all itemsets from local heaps (workers)
        # Note: initial itemsets are already in each local heap, so we don't insert them again
        for local_mh, _ in local_results:
            for support, itemset in local_mh.get_all():
                merged_heap.insert(support=support, itemset=itemset)
        
        # If no local results, use initial heap as fallback
        if not merged_heap.get_all() and initial_min_heap.get_all():
            for support, itemset in initial_min_heap.get_all():
                merged_heap.insert(support=support, itemset=itemset)
        
        # Get final rmsup (minimum support in top-k)
        final_rmsup = merged_heap.min_support()
        
        return merged_heap, final_rmsup
    
    @staticmethod
    def _copy_min_heap(min_heap: MinHeapTopK) -> MinHeapTopK:
        """
        Create a thread-safe copy of a min-heap.
        
        Each worker needs its own independent copy of the heap
        to avoid concurrent access to the shared heap.
        
        Args:
            min_heap: MinHeapTopK object to copy
        
        Returns:
            New MinHeapTopK with same itemsets and k value
        """
        new_heap = MinHeapTopK(min_heap.k)
        
        # Copy all itemsets from source heap
        for support, itemset in min_heap.get_all():
            new_heap.insert(support=support, itemset=itemset)
        
        return new_heap
    
    def shutdown(self):
        """
        Shutdown the thread pool and wait for all threads to complete.
        
        Should be called after process_partitions() completes to cleanup resources.
        """
        self.thread_pool.shutdown(wait=True)
    
    def __enter__(self):
        """Context manager entry"""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - cleanup thread pool"""
        self.shutdown()
        return False
