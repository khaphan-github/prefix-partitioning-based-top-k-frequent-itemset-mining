"""
SglPartitionHybridCandidatePruning: Enhanced version with Candidate Pruning.

This is an improved implementation of Algorithm 2 (ProcessSglPartition) that uses:
1. Hybrid Vertical Storage (tid-list, dif-list, or bit-vector)
2. Candidate Pruning (Timeliness + Last-item pruning)

Candidate Pruning reduces the number of candidate itemsets that require tid-set
intersection operations, which are CPU/I/O intensive on vertical data.

Two pruning techniques:
- Timeliness Pruning: Skip candidates whose support <= current rmsup
- Last-item Pruning: Skip pairs {y1, y2} if {y1, y2} is not in top-k

Both techniques guarantee:
- No false negatives (don't miss top-k itemsets)
- No false positives (no spurious itemsets)
"""

import heapq
from typing import List, Dict, Tuple, Set
from ptf.min_heap import MinHeapTopK
from ptf.hybrid_vertical_storage import (
    TidSetEntry,
    HybridVerticalIndex,
    HybridTidSetIntersection,
)


class SglPartitionHybridCandidatePruning:
    """
    Algorithm 2: ProcessSglPartition with Hybrid Storage and Candidate Pruning.
    
    Enhanced version that combines:
    1. Optimized tid-set storage formats (tid-list, dif-list, bit-vector)
    2. Timeliness pruning (based on support threshold)
    3. Last-item pruning (based on 2-itemsets in top-k)
    
    Input:
        - partition_item: The prefix item xi
        - promising_items: AR_i list of promising items for this partition
        - partition_data: Raw transaction data for partition Pi
        - min_heap: Top-k itemsets found so far
        - rmsup: Running minimum support (threshold)
        - top2_set: Set of 2-itemsets currently in top-k (for last-item pruning)
    
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
        top2_set: Set[frozenset] = None,
    ) -> Tuple[MinHeapTopK, int, Set[frozenset]]:
        """
        Execute Algorithm 2 with hybrid storage and candidate pruning.
        
        Args:
            partition_item: The prefix item xi
            promising_items: List of promising items (AR_i) including prefix
            partition_data: Transactions in partition Pi (raw list of lists)
            min_heap: Current top-k itemsets (MinHeapTopK object)
            rmsup: Current running minimum support threshold
            top2_set: Set of frozenset({a,b}) for 2-itemsets in top-k
                     If None, will be computed from min_heap
        
        Returns:
            Tuple of (updated_min_heap, updated_rmsup, updated_top2_set)
        """
        partition_size = len(partition_data)
        
        # Initialize top2_set if not provided
        if top2_set is None:
            top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
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
        return SglPartitionHybridCandidatePruning._execute_with_vertical_index(
            partition_item=partition_item,
            promising_items=promising_items,
            vertical_index=vertical_index,
            min_heap=min_heap,
            rmsup=rmsup,
            partition_size=partition_size,
            top2_set=top2_set
        )
    
    @staticmethod
    def execute_with_vertical_index(
        partition_item: int,
        promising_items: List[int],
        vertical_index: HybridVerticalIndex,
        min_heap: MinHeapTopK,
        rmsup: int,
        top2_set: Set[frozenset] = None,
    ) -> Tuple[MinHeapTopK, int, Set[frozenset]]:
        """
        Execute Algorithm 2 with pre-computed HybridVerticalIndex.
        
        Args:
            partition_item: The prefix item xi
            promising_items: List of promising items (AR_i) including prefix
            vertical_index: Pre-built hybrid vertical index
            min_heap: Current top-k itemsets (MinHeapTopK object)
            rmsup: Current running minimum support threshold
            top2_set: Set of frozenset({a,b}) for 2-itemsets in top-k
                     If None, will be computed from min_heap
        
        Returns:
            Tuple of (updated_min_heap, updated_rmsup, updated_top2_set)
        """
        # Initialize top2_set if not provided
        if top2_set is None:
            top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        return SglPartitionHybridCandidatePruning._execute_with_vertical_index(
            partition_item=partition_item,
            promising_items=promising_items,
            vertical_index=vertical_index,
            min_heap=min_heap,
            rmsup=rmsup,
            partition_size=vertical_index.ni,
            top2_set=top2_set
        )
    
    @staticmethod
    def _execute_with_vertical_index(
        partition_item: int,
        promising_items: List[int],
        vertical_index: HybridVerticalIndex,
        min_heap: MinHeapTopK,
        rmsup: int,
        partition_size: int,
        top2_set: Set[frozenset],
    ) -> Tuple[MinHeapTopK, int, Set[frozenset]]:
        """
        Internal method: Execute Algorithm 2 with HybridVerticalIndex and Candidate Pruning.
        
        This is the main algorithm combining hybrid tid-set storage with two pruning techniques:
        1. Timeliness Pruning: Check support(X∪{y2}) <= rmsup before intersection
        2. Last-item Pruning: Check if {y1, y2} is in top-k 2-itemsets before intersection
        """
        
        # PHASE 1: Initialize 2-itemsets
        # ============================================================
        # HT: Hash table mapping frozenset(itemset) -> HTEntry with support info
        ht = {}  # frozenset(itemset) -> {'entry': TidSetEntry, 'support': int}
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
                
                # Store both TidSetEntry and support in HT
                ht[itemset_key] = {
                    'entry': entry_pair,
                    'support': support_pair
                }
                
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
                old_rmsup = rmsup
                rmsup = min_heap.min_support()
                
                # If rmsup increased, update top2_set
                if rmsup > old_rmsup:
                    top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
            
            # Step 2.4-2.9: Try extending itemset with remaining items
            # Only extend with items that come after last item in AR_i
            itemset_list = sorted(list(itemset_rt))
            last_item = itemset_list[-1]
            y1 = last_item  # For last-item pruning
            
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
                
                # ===== PRUNING 1: TIMELINESS PRUNING =====
                # If support(X∪{y2}) <= rmsup, skip
                # because support(X∪{y1,y2}) <= support(X∪{y2}) <= rmsup
                ht_entry_with_y2 = ht[itemset_with_y2]
                if ht_entry_with_y2['support'] <= rmsup:
                    continue
                
                # ===== PRUNING 2: LAST-ITEM PRUNING =====
                # If {y1, y2} not in top-k 2-itemsets, skip
                # because any superset containing {y1,y2} cannot be top-k
                pair_key = frozenset([y1, y2])
                if pair_key not in top2_set:
                    continue
                
                # Step 2.6: Calculate tid-set intersection
                entry_last_y2 = ht_entry_with_y2['entry']
                entry_rt = ht[itemset_rt]['entry']
                
                if entry_rt is None or entry_last_y2 is None:
                    continue
                
                # Use hybrid intersection dispatcher
                entry_new, support_new = HybridTidSetIntersection.intersect(
                    entry_rt, entry_last_y2, partition_size)
                
                # Step 2.7: Check support threshold
                if support_new > rmsup:
                    # Step 2.8: Add new itemset to HT and QE
                    itemset_new = itemset_rt | {y2}
                    ht[itemset_new] = {
                        'entry': entry_new,
                        'support': support_new
                    }
                    heapq.heappush(qe, (-support_new, itemset_new))
        
        return min_heap, rmsup, top2_set
    
    @staticmethod
    def _extract_top2_itemsets(min_heap: MinHeapTopK) -> Set[frozenset]:
        """
        Extract all 2-itemsets from min_heap into a set for O(1) lookup.
        
        Args:
            min_heap: MinHeapTopK containing top-k itemsets
        
        Returns:
            Set of frozenset({a, b}) for all 2-itemsets in min_heap
        """
        top2_set = set()
        
        for support, itemset in min_heap.get_all():
            if len(itemset) == 2:
                # Store as frozenset for O(1) set membership check
                pair = frozenset(itemset)
                top2_set.add(pair)
        
        return top2_set
    
    @staticmethod
    def get_pruning_stats(
        partition_item: int,
        promising_items: List[int],
        partition_data: List[List[int]],
        min_heap: MinHeapTopK,
        rmsup: int,
        top2_set: Set[frozenset] = None,
    ) -> Dict:
        """
        Execute algorithm and return statistics about pruning effectiveness.
        
        Returns:
            Dictionary with:
            - intersections_performed: Number of actual intersections done
            - candidates_before_timeliness: Number before timeliness pruning
            - candidates_after_timeliness: Number after timeliness pruning
            - candidates_after_lastitem: Number after last-item pruning
            - timeliness_pruning_ratio: Percentage pruned by timeliness
            - lastitem_pruning_ratio: Percentage pruned by last-item
        """
        partition_size = len(partition_data)
        
        if top2_set is None:
            top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        # Build vertical index
        vertical_index = HybridVerticalIndex(
            partition_item=partition_item,
            ni=partition_size
        )
        vertical_index.build_from_partition(
            partition_data=partition_data,
            promising_items=promising_items
        )
        
        # Track pruning statistics
        stats = {
            'intersections_performed': 0,
            'candidates_before_timeliness': 0,
            'candidates_after_timeliness': 0,
            'candidates_after_lastitem': 0,
            'timeliness_pruning_count': 0,
            'lastitem_pruning_count': 0,
        }
        
        # Run algorithm with stat tracking
        ht = {}
        qe = []
        
        # Phase 1: Initialize 2-itemsets
        for j in range(1, len(promising_items)):
            xj = promising_items[j]
            
            if not vertical_index.contains_item(partition_item) or not vertical_index.contains_item(xj):
                continue
            
            entry_xi = vertical_index.get_entry(partition_item)
            entry_xj = vertical_index.get_entry(xj)
            
            entry_pair, support_pair = HybridTidSetIntersection.intersect(
                entry_xi, entry_xj, partition_size)
            
            if support_pair > rmsup:
                itemset_key = frozenset([partition_item, xj])
                ht[itemset_key] = {
                    'entry': entry_pair,
                    'support': support_pair
                }
                heapq.heappush(qe, (-support_pair, itemset_key))
        
        # Phase 2: Main loop with stat tracking
        while qe:
            neg_support_rt, itemset_rt = heapq.heappop(qe)
            support_rt = -neg_support_rt
            
            if support_rt <= rmsup:
                break
            
            if len(itemset_rt) >= 3:
                itemset_tuple = tuple(sorted(itemset_rt))
                min_heap.insert(support=support_rt, itemset=itemset_tuple)
                old_rmsup = rmsup
                rmsup = min_heap.min_support()
                
                if rmsup > old_rmsup:
                    top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
            
            itemset_list = sorted(list(itemset_rt))
            last_item = itemset_list[-1]
            y1 = last_item
            
            try:
                last_pos = promising_items.index(last_item)
            except ValueError:
                continue
            
            for next_pos in range(last_pos + 1, len(promising_items)):
                y2 = promising_items[next_pos]
                
                itemset_without_last = frozenset(itemset_rt - {last_item})
                itemset_with_y2 = itemset_without_last | {y2}
                
                if itemset_with_y2 not in ht:
                    continue
                
                stats['candidates_before_timeliness'] += 1
                
                # Timeliness pruning check
                ht_entry_with_y2 = ht[itemset_with_y2]
                if ht_entry_with_y2['support'] <= rmsup:
                    stats['timeliness_pruning_count'] += 1
                    continue
                
                stats['candidates_after_timeliness'] += 1
                
                # Last-item pruning check
                pair_key = frozenset([y1, y2])
                if pair_key not in top2_set:
                    stats['lastitem_pruning_count'] += 1
                    continue
                
                stats['candidates_after_lastitem'] += 1
                
                # Perform intersection
                entry_last_y2 = ht_entry_with_y2['entry']
                entry_rt = ht[itemset_rt]['entry']
                
                if entry_rt is None or entry_last_y2 is None:
                    continue
                
                entry_new, support_new = HybridTidSetIntersection.intersect(
                    entry_rt, entry_last_y2, partition_size)
                
                stats['intersections_performed'] += 1
                
                if support_new > rmsup:
                    itemset_new = itemset_rt | {y2}
                    ht[itemset_new] = {
                        'entry': entry_new,
                        'support': support_new
                    }
                    heapq.heappush(qe, (-support_new, itemset_new))
        
        # Calculate ratios
        if stats['candidates_before_timeliness'] > 0:
            stats['timeliness_pruning_ratio'] = (
                stats['timeliness_pruning_count'] / 
                stats['candidates_before_timeliness']
            )
        else:
            stats['timeliness_pruning_ratio'] = 0.0
        
        if stats['candidates_after_timeliness'] > 0:
            stats['lastitem_pruning_ratio'] = (
                stats['lastitem_pruning_count'] / 
                stats['candidates_after_timeliness']
            )
        else:
            stats['lastitem_pruning_ratio'] = 0.0
        
        return stats
