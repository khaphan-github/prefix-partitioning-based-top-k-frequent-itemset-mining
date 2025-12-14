'''
'''


from typing import List, Tuple
from ptf.min_heap import MinHeapTopK
from ptf.sgl_partition import SglPartition


class PrefixPartitioningbasedTopKAlgorithm:
    def __init__(self, top_k: int,):
        self.top_k = top_k

    def initialize_mh_and_rmsup(self, con_list: List[Tuple[set, int]]):
        '''
        Min heap lay top k item trong co-occurrence list
        rmsub la item dau tien.
        '''

        min_heap = MinHeapTopK(self.top_k)

        for con in range(min(self.top_k, len(con_list))):
            itemset, support = con_list[con]
            min_heap.insert(support=support, itemset=tuple(itemset))
        rmsup = min_heap.min_support()

        return min_heap, rmsup

    def build_promissing_item_arrays(self, min_heap: MinHeapTopK, all_items):
        '''
        output:  [{ item1, item2, ...}, ...]
        {1: [], 2: [4, 10, 2], 3: [], 4: [4], 5: [], 6: [], 7: [], 8: [], 9: [10, 2, 9], 10: [10]}
        '''
        promising_items_arr = {}

        for item in all_items:
            promising_items_arr[item] = []

        # Add items from min_heap
        for support, itemset in min_heap.get_all():
            if len(itemset) == 1:
                x_i = itemset[0]
                promising_items_arr[x_i].append(x_i)

            if len(itemset) == 2:
                x_i, x_j = itemset
                promising_items_arr[x_i].append(x_j)

        return promising_items_arr

    def filter_partitions(self, ar: dict, all_items: List[int], min_heap: MinHeapTopK, con_map: dict, rmsup: int):
        '''
        Filter partitions based on pruning conditions:
        1. If partition_item support <= rmsup, skip partition
        2. If 2-itemset {x_i, x_j} support <= rmsup, remove x_j from AR[x_i]
        3. If |AR[x_i]| <= 2, skip partition (Theorem 2)

        Output: partitions_to_process (list of partition items worth processing)
        '''
        partitions_to_process = []

        for partition_item in all_items:
            promising_items = ar[partition_item]

            # Pruning 1: Check if partition prefix itself has sufficient support
            prefix_support = con_map.get(frozenset([partition_item]), 0)
            if prefix_support <= rmsup:
                ar[partition_item] = []
                continue

            # Pruning 2: Filter promising items based on 2-itemset support
            filtered_promising_items = []
            for promising_item in promising_items:
                # max_sub_partition_item = support of 2-itemset {partition_item, promising_item}
                two_itemset = frozenset([partition_item, promising_item])
                max_sub_partition_item = con_map.get(two_itemset, 0)

                # Keep only if 2-itemset support > rmsup
                if max_sub_partition_item > rmsup:
                    filtered_promising_items.append(promising_item)

            ar[partition_item] = filtered_promising_items

            # Pruning 3: Skip if not enough items to form complex itemsets
            if len(filtered_promising_items) <= 2:
                continue
            else:
                # Partition passes all filters, add to processing list
                partitions_to_process.append(partition_item)
                SglPartition.execute(
                    partition_item, filtered_promising_items, min_heap)

        return partitions_to_process
