"""
SglPartitionHybrid: Enhanced version of SglPartition using Hybrid Vertical Storage.

This is an improved implementation of Algorithm 2 (ProcessSglPartition) that uses
hybrid tid-set storage to reduce I/O and memory consumption. It produces identical
results to SglPartition but with better performance characteristics.

Key differences from SglPartition:
- Uses HybridVerticalIndex instead of dict-based tidset_map
- Uses HybridTidSetIntersection for all 6 tid-set format combinations
- Automatically selects optimal storage format for each item
- Provides memory usage statistics and comparison with naive approach
"""

import heapq
from typing import List, Dict, Tuple
from ptf.min_heap import MinHeapTopK
from ptf.hybrid_vertical_storage import (
    TidSetEntry,
    HybridVerticalIndex,
    HybridTidSetIntersection,
)


class SglPartitionHybrid:
    """
    Algorithm 2: ProcessSglPartition with Hybrid Vertical Storage.
    
    Enhanced version that uses optimized tid-set storage formats (tid-list,
    dif-list, or bit-vector) selected automatically for each item based on
    memory efficiency.
    
    Input:
        - partition_item: The prefix item xi
        - promising_items: AR_i list of promising items for this partition
        - partition_data: Raw transaction data for partition Pi
        - min_heap: Top-k itemsets found so far
        - rmsup: Running minimum support (threshold)
    
    Output:
        - Updated min_heap with new frequent itemsets found
        - Updated rmsup (may increase as better itemsets are found)
    """
    
    @staticmethod
    def execute(
        partition_item: int,
        promising_items: List[int],
        partition_data: List[List[int]],
        min_heap: MinHeapTopK,
        rmsup: int,
    ) -> Tuple[MinHeapTopK, int]:
        """
        Execute Algorithm 2 using hybrid vertical storage.
        
        Args:
            partition_item: The prefix item xi
            promising_items: List of promising items (AR_i) including prefix
            partition_data: Transactions in partition Pi (raw list of lists)
            min_heap: Current top-k itemsets (MinHeapTopK object)
            rmsup: Current running minimum support threshold
        
        Returns:
            Tuple of (updated_min_heap, updated_rmsup)
        """
        partition_size = len(partition_data)
        
        # Build hybrid vertical index from raw transaction data
        vertical_index = HybridVerticalIndex(
            partition_item=partition_item,
            ni=partition_size
        )
        vertical_index.build_from_partition(
            partition_data=partition_data,
            promising_items=promising_items
        )
        
        # Process using the internal method
        return SglPartitionHybrid._execute_with_vertical_index(
            partition_item=partition_item,
            promising_items=promising_items,
            vertical_index=vertical_index,
            min_heap=min_heap,
            rmsup=rmsup,
            partition_size=partition_size
        )
    
    @staticmethod
    def execute_with_vertical_index(
        partition_item: int,
        promising_items: List[int],
        vertical_index: HybridVerticalIndex,
        min_heap: MinHeapTopK,
        rmsup: int,
    ) -> Tuple[MinHeapTopK, int]:
        """
        Execute Algorithm 2 with pre-computed HybridVerticalIndex (for advanced usage).
        
        Args:
            partition_item: The prefix item xi
            promising_items: List of promising items (AR_i) including prefix
            vertical_index: Pre-built hybrid vertical index
            min_heap: Current top-k itemsets (MinHeapTopK object)
            rmsup: Current running minimum support threshold
        
        Returns:
            Tuple of (updated_min_heap, updated_rmsup)
        """
        return SglPartitionHybrid._execute_with_vertical_index(
            partition_item=partition_item,
            promising_items=promising_items,
            vertical_index=vertical_index,
            min_heap=min_heap,
            rmsup=rmsup,
            partition_size=vertical_index.ni
        )
    
    @staticmethod
    def _execute_with_vertical_index(
        partition_item: int,
        promising_items: List[int],
        vertical_index: HybridVerticalIndex,
        min_heap: MinHeapTopK,
        rmsup: int,
        partition_size: int
    ) -> Tuple[MinHeapTopK, int]:
        """
        Internal method: Execute Algorithm 2 with HybridVerticalIndex.
        This is the main algorithm using hybrid tid-set storage.
        """
        
        # PHASE 1: Initialize 2-itemsets
        # ============================================================
        ht = {}  # Hash table: frozenset(itemset) -> TidSetEntry
        qe = []  # Priority queue (max-heap): [(-support, itemset), ...]
        
        # Create initial 2-itemsets from promising items
        # For each pair (xi, xj) where xi is prefix and xj in AR_i
        for j in range(1, len(promising_items)):
            xj = promising_items[j]
            
            # Get tid-set entries from vertical index
            if not vertical_index.contains_item(partition_item) or not vertical_index.contains_item(xj):
                continue
            
            entry_xi = vertical_index.get_entry(partition_item)
            entry_xj = vertical_index.get_entry(xj)
            
            # Calculate tid-set intersection using hybrid dispatcher
            entry_pair, support_pair = HybridTidSetIntersection.intersect(
                entry_xi, entry_xj, partition_size)
            
            if support_pair > rmsup:
                # Create itemset key (frozenset for hashing)
                itemset_key = frozenset([partition_item, xj])
                # Store the TidSetEntry in HT
                entry_pair.item = -1  # Placeholder for combined itemset
                ht[itemset_key] = entry_pair
                
                # Add to priority queue (use negative support for max-heap)
                heapq.heappush(qe, (-support_pair, itemset_key))
        
        # PHASE 2: Main loop - Expand itemsets high-support-first
        # ============================================================
        while qe:
            # Step 2.1: Pop itemset with maximum support
            neg_support_rt, itemset_rt = heapq.heappop(qe)
            support_rt = -neg_support_rt
            
            # Step 2.2: Check termination condition
            # If max support in queue <= rmsup, no more itemsets can be top-k
            if support_rt <= rmsup:
                break
            
            # Step 2.3: Update MH if itemset size >= 3
            # Only 3+ itemsets are candidates for top-k
            if len(itemset_rt) >= 3:
                # Convert frozenset back to sorted tuple for heap
                itemset_tuple = tuple(sorted(itemset_rt))
                min_heap.insert(support=support_rt, itemset=itemset_tuple)
                
                # Update rmsup (minimum support in top-k)
                rmsup = min_heap.min_support()
            
            # Step 2.4-2.9: Try extending itemset with remaining items
            # Only extend with items that come after last item in AR_i
            itemset_list = sorted(list(itemset_rt))
            last_item = itemset_list[-1]
            
            try:
                # Find position of last_item in promising_items
                last_pos = promising_items.index(last_item)
            except ValueError:
                # last_item not in promising_items, skip
                continue
            
            # Try extending with items after last_pos in promising_items
            for next_pos in range(last_pos + 1, len(promising_items)):
                y2 = promising_items[next_pos]
                
                # Step 2.5: Check if X ∪ {y2} exists in HT
                # where X = itemset_rt \ {last_item}
                itemset_without_last = frozenset(itemset_rt - {last_item})
                itemset_with_y2 = itemset_without_last | {y2}
                
                # By Theorem 3: If itemset_with_y2 not in HT,
                # then support(itemset_rt + {y2}) <= rmsup
                if itemset_with_y2 not in ht:
                    continue
                
                # Step 2.6: Calculate tid-set intersection
                entry_last_y2 = ht.get(itemset_with_y2)
                entry_rt = ht.get(itemset_rt)
                
                if entry_rt is None or entry_last_y2 is None:
                    continue
                
                # Use hybrid intersection dispatcher
                entry_new, support_new = HybridTidSetIntersection.intersect(
                    entry_rt, entry_last_y2, partition_size)
                
                # Step 2.7: Check support threshold
                if support_new > rmsup:
                    # Step 2.8: Add new itemset to HT and QE
                    itemset_new = itemset_rt | {y2}
                    entry_new.item = -1  # Placeholder for combined itemset
                    ht[itemset_new] = entry_new
                    heapq.heappush(qe, (-support_new, itemset_new))
        
        return min_heap, rmsup
    
    @staticmethod
    def get_vertical_index_stats(
        partition_item: int,
        promising_items: List[int],
        partition_data: List[List[int]],
    ) -> Dict:
        """
        Build vertical index and return statistics about format distribution.
        
        Useful for understanding memory savings in a partition.
        
        Args:
            partition_item: The prefix item xi
            promising_items: List of promising items
            partition_data: Transactions in partition
        
        Returns:
            Dictionary with format statistics and memory comparison
        """
        vertical_index = HybridVerticalIndex(
            partition_item=partition_item,
            ni=len(partition_data)
        )
        vertical_index.build_from_partition(
            partition_data=partition_data,
            promising_items=promising_items
        )
        
        stats = vertical_index.get_stats()
        naive_bytes, reduction = vertical_index.memory_comparison()
        
        stats['naive_bytes'] = naive_bytes
        stats['reduction_factor'] = reduction
        
        return stats
