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
                    partition=partition,
                    promising_items=promissing_arr[partition],
                    min_heap=min_heap
                )
