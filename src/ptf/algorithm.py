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

        TODO:
        Itemset: (2, 4), Support: 3
        Itemset: (4,), Support: 4
        Itemset: (9, 10), Support: 5
        Itemset: (9, 2), Support: 7
        Itemset: (2, 10), Support: 8
        Itemset: (10,), Support: 9
        Itemset: (9,), Support: 7
        Itemset: (2,), Support: 10
        {1: [], 2: [4, 10, 2], 3: [], 4: [4], 5: [], 6: [], 7: [], 8: [], 9: [10, 2, 9], 10: [10]}
        '''
        promising_items_arr = {}

        for item in all_items:
            promising_items_arr[item] = []

        # Add items from min_heap
        for support, itemset in min_heap.get_all():
            print(f"Itemset: {itemset}, Support: {support}")
            if len(itemset) == 1:
                x_i = itemset[0]
                promising_items_arr[x_i].append(x_i)

            if len(itemset) == 2:
                x_i, x_j = itemset
                promising_items_arr[x_i].append(x_j)

        return promising_items_arr

    def filter_partitions(self, ar: dict, all_items: List[int], min_heap: MinHeapTopK, rmsup):
        '''
        '''
        for partition_item in all_items:
            promising_items = ar[partition_item]
            for promissing_item in promising_items:
                # TODO: Implement algorithms
                min_sub_partition_item = -1
                if promissing_item == partition_item or min_sub_partition_item <= rmsup:
                    ar[partition_item] = []
                    break

            if len(promising_items) <= 2:
                continue
            else:
                SglPartition.execute(partition_item, promising_items, min_heap)
