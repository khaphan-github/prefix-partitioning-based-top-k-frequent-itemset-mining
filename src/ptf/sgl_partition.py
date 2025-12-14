import heapq
from typing import List, Dict, Tuple, Set
from ptf.min_heap import MinHeapTopK


class SglPartition:
    '''
    Algorithm 2: ProcessSglPartition

    Process a single partition using vertical mining with high-support-first principle.

    Input:
        - partition_item: The prefix item x_i
        - promising_items: AR_i list of promising items for this partition
        - tidset_map: Dict mapping items to their tid-sets
        - min_heap: Top-k itemsets found so far
        - rmsup: Running minimum support (threshold)

    Output:
        - Updated min_heap with new frequent itemsets found
        - Updated rmsup (may increase as better itemsets are found)
    '''

    @staticmethod
    def execute(
        partition_item: int,
        promising_items: List[int],
        tidset_map: Dict[int, List[int]],
        min_heap: MinHeapTopK,
        rmsup: int,
        partition_size: int = None
    ) -> Tuple[MinHeapTopK, int]:
        '''
        Execute Algorithm 2 for processing a single partition.

        Args:
            partition_item: The prefix item x_i
            promising_items: List of promising items (AR_i) including prefix
            tidset_map: Mapping from item to tid-set (sorted lists of transaction IDs)
            min_heap: Current top-k itemsets (MinHeapTopK object)
            rmsup: Current running minimum support threshold
            partition_size: Total transactions in partition (optional)

        Returns:
            Tuple of (updated_min_heap, updated_rmsup)
        '''

        # PHASE 1: Initialize 2-itemsets
        # ============================================================
        ht = {}  # Hash table: frozenset(itemset) -> tid_set
        qe = []  # Priority queue (max-heap): [(-support, itemset), ...]

        # Create initial 2-itemsets from promising items
        # For each pair (xi, xj) where xi is prefix and xj in AR_i
        for j in range(1, len(promising_items)):
            xj = promising_items[j]

            # Get tid-sets for intersection
            tidset_xi = tidset_map.get(partition_item, [])
            tidset_xj = tidset_map.get(xj, [])

            if not tidset_xi or not tidset_xj:
                continue

            # Calculate tid-set intersection
            tidset_pair = SglPartition._tidset_intersection(
                tidset_xi, tidset_xj)
            support_pair = len(tidset_pair)

            if support_pair > rmsup:
                # Create itemset key (frozenset for hashing)
                itemset_key = frozenset([partition_item, xj])
                ht[itemset_key] = tidset_pair

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
                tidset_last_y2 = ht.get(itemset_with_y2, [])
                tidset_rt = ht.get(itemset_rt, [])

                if not tidset_rt or not tidset_last_y2:
                    continue

                tidset_new = SglPartition._tidset_intersection(
                    tidset_rt, tidset_last_y2)
                support_new = len(tidset_new)

                # Step 2.7: Check support threshold
                if support_new > rmsup:
                    # Step 2.8: Add new itemset to HT and QE
                    itemset_new = itemset_rt | {y2}
                    ht[itemset_new] = tidset_new
                    heapq.heappush(qe, (-support_new, itemset_new))

        return min_heap, rmsup

    @staticmethod
    def _tidset_intersection(tidset1: List[int], tidset2: List[int]) -> List[int]:
        '''
        Efficient intersection of two sorted tid-lists using binary merge.

        Both tid-sets should be pre-sorted in ascending order.
        Time complexity: O(n + m) where n, m are sizes of tid-sets

        Args:
            tidset1: Sorted list of transaction IDs
            tidset2: Sorted list of transaction IDs

        Returns:
            Sorted list of transaction IDs in both tidset1 and tidset2
        '''
        result = []
        i, j = 0, 0

        while i < len(tidset1) and j < len(tidset2):
            if tidset1[i] == tidset2[j]:
                result.append(tidset1[i])
                i += 1
                j += 1
            elif tidset1[i] < tidset2[j]:
                i += 1
            else:
                j += 1

        return result
