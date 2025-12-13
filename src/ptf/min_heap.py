import heapq
from typing import Dict, List, Tuple
'''
  min heap MH of size k (initially empty) to maintain the k most frequent itemsets discovered so far
'''


class MinHeapTopK:
    def __init__(self, k: int):
        self.k = k
        self.heap: List[Tuple[int, Tuple]] = []  # (support, itemset)
        self.itemset_map: Dict[Tuple, int] = {}  # itemset -> support

    def insert(self, support: int, itemset: Tuple):
        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (support, itemset))
            self.itemset_map[itemset] = support
        # elif support > self.heap[0][0]:
        #     old_support, old_itemset = heapq.heappushpop(
        #         self.heap, (support, itemset))
        #     del self.itemset_map[old_itemset]
        #     self.itemset_map[itemset] = support

    def min_support(self) -> int:
        return self.heap[0][0] if self.heap else 0

    def is_full(self) -> bool:
        return len(self.heap) == self.k

    def get_all(self) -> List[Tuple]:
        return self.heap
