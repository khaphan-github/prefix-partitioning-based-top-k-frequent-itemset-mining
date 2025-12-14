'''
'''


from typing import List, Tuple, Dict
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

    def filter_partitions(self, promissing_arr: dict, partitions: List[int], min_heap: MinHeapTopK, con_map: dict, rmsup: int):
        '''
        '''
        for partition in partitions:
            # Promising items in current partition
            for promissing_item in promissing_arr[partition]:
                partition_support = con_map.get((partition,), 0)
                if promissing_item == partition and partition_support <= rmsup:
                    promissing_arr[partition] = []
                    break

                if promissing_item > partition:
                    pair_key = (partition, promissing_item)
                    pair_support = con_map.get(pair_key, 0)
                    if pair_support <= rmsup:
                        promissing_arr[partition].remove(promissing_item)

            if len(promissing_arr[partition]) <= 2:
                continue
            else:
                SglPartition.execute(
                    partition_item=partition,
                    promising_items=promissing_arr[partition],
                    tidset_map={},  # Will be populated with vertical representation
                    min_heap=min_heap,
                    rmsup=rmsup
                )

    def build_vertical_representation(self, partition_data: List[List[int]], partition_item: int, promising_arr: List[int]):
        '''
        Build vertical representation (tidsets) for items in a partition.

        Input:
            partition_data (List[List[int]]): Transactions in partition P_i
            partition_item (int): The prefix item x_i
            ar_i (List[int]): Promising items for this partition

        Output:
            vertical_rep (VerticalRepresentation): Vertical representation object with tidsets
            tidset_map (Dict): Direct mapping from item to tid-set for algorithm efficiency

        Process:
            1. Assign local TID to each transaction in partition (0-indexed)
            2. For each item in AR_i (excluding x_i):
               a. Create tid-set as list of local TIDs containing that item
               b. Sort tid-set in ascending order (for efficient intersection)
            3. Store both in VerticalRepresentation and return direct tidset_map
        '''
        tidset_map = {}

        # Initialize tidset for all promising items (excluding partition_item)
        for item in promising_arr:
            if item != partition_item:
                tidset_map[item] = []
            else:
                # Prefix item appears in all transactions
                tidset_map[item] = list(range(len(partition_data)))

        # Assign local TID and build tidsets
        for local_tid, transaction in enumerate(partition_data):
            for item in transaction:
                if item in tidset_map and item != partition_item:
                    tidset_map[item].append(local_tid)

        # Sort tidsets for efficient intersection operations
        for item in tidset_map:
            tidset_map[item].sort()

        partition_size = len(partition_data)

        return tidset_map, partition_size
