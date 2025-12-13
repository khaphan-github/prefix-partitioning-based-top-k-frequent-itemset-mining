'''
'''


from typing import List, Tuple
from ptf.min_heap import MinHeapTopK


class PrefixPartitioningbasedTopKAlgorithm:
    def __init__(self, top_k: int,):
        self.top_k = top_k

    def initialize_mh_and_rmsup(self, con_list: List[Tuple[set, int]]):
        min_heap = MinHeapTopK(self.top_k)
        for con in range(min(self.top_k, len(con_list))):
            itemset, support = con_list[con]
            min_heap.insert(support=support, itemset=tuple(itemset))
        rmsup = min_heap.min_support()
        return min_heap, rmsup

      