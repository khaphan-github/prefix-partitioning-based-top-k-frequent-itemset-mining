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

    def build_promissing_item_arrays(self, min_heap: MinHeapTopK, all_items, con_map: dict, rmsup: int):
        '''
        Build promising items arrays with refinement.
        
        Step 1: Add items from min_heap (1-itemset and 2-itemset)
        Step 2: Refine: Keep only items with sufficient support
        
        output:  {item: [promising_items], ...}
        '''
        promising_items_arr = {}

        for item in all_items:
            promising_items_arr[item] = []

        # Step 1: Add items from min_heap
        for support, itemset in min_heap.get_all():
            if len(itemset) == 1:
                x_i = itemset[0]
                promising_items_arr[x_i].append(x_i)

            elif len(itemset) == 2:
                # Ensure x_i < x_j for consistent partition assignment
                items = sorted(itemset)
                x_i, x_j = items[0], items[1]
                promising_items_arr[x_i].append(x_j)

        # Step 2: Refine AR_i based on rmsup and con_map
        for x_i in all_items:
            new_arr = []
            for y in promising_items_arr[x_i]:
                # Get support for item y in partition P_{x_i}
                if y == x_i:
                    # Check support of 1-itemset {x_i}
                    sup = con_map.get(frozenset([x_i]), 0)
                else:
                    # Check support of 2-itemset {x_i, y}
                    sup = con_map.get(frozenset([x_i, y]), 0)
                
                # Keep item only if support >= rmsup
                if sup >= rmsup:
                    new_arr.append(y)
            
            promising_items_arr[x_i] = sorted(set(new_arr))

        return promising_items_arr

    def filter_partitions(self, promissing_arr: dict, partitions: List[int], min_heap: MinHeapTopK, con_map: dict, rmsup: int, partitioner=None):
        '''
        Filter partitions and process those that meet criteria.

        For each partition:
        1. Remove non-promising items based on support threshold
        2. Skip if |AR_i| <= 2
        3. Otherwise, build vertical representation and process the partition
        
        Returns:
            Tuple (min_heap, rmsup): Updated heap and minimum support threshold
        '''
        for partition in partitions:
            # Promising items in current partition
            for promissing_item in promissing_arr[partition]:
                partition_support = con_map.get((partition,), 0)
                if promissing_item == partition and partition_support <= rmsup:
                    # TODO: Tai sao khogn remove buoc nay.
                    # promissing_arr[partition] = []
                    break

                if promissing_item > partition:
                    pair_key = (partition, promissing_item)
                    pair_support = con_map.get(pair_key, 0)
                    if pair_support <= rmsup:
                        promissing_arr[partition].remove(promissing_item)

            # Skip partition if promising items <= 2
            if len(promissing_arr[partition]) <= 2:
                continue

            # Process the partition with partition data
            if partitioner and hasattr(partitioner, 'prefix_partitions'):
                partition_data = partitioner.prefix_partitions.get(partition, [])

                # Process the partition (vertical representation built inside execute)
                min_heap, rmsup = SglPartition.execute(
                    partition_item=partition,
                    promising_items=promissing_arr[partition],
                    partition_data=partition_data,
                    min_heap=min_heap,
                    rmsup=rmsup
                )
        
        return min_heap, rmsup
