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
        """
        Insert itemset with support into the min-heap.
        Maintains top-k invariant: heap size <= k
        Handles duplicates by keeping the highest support value.
        """
        # If itemset already exists, update to higher support if needed
        if itemset in self.itemset_map:
            current_support = self.itemset_map[itemset]
            if support > current_support:
                # Update to higher support
                self.itemset_map[itemset] = support
                # Rebuild heap with updated values
                self.heap = [(self.itemset_map.get(item, sup), item) 
                             for sup, item in self.heap]
                heapq.heapify(self.heap)
            return
        
        if len(self.heap) < self.k:
            heapq.heappush(self.heap, (support, itemset))
            self.itemset_map[itemset] = support
        elif support > self.heap[0][0]:
            # Replace minimum element with new higher-support itemset
            old_support, old_itemset = heapq.heappushpop(
                self.heap, (support, itemset))
            if old_itemset in self.itemset_map:
                del self.itemset_map[old_itemset]
            self.itemset_map[itemset] = support

    def min_support(self) -> int:
        """Get minimum support value in heap (smallest element)"""
        return self.heap[0][0] if self.heap else 0

    def is_full(self) -> bool:
        """Check if heap has reached capacity k"""
        return len(self.heap) == self.k

    def get_all(self) -> List[Tuple]:
        """Get all items in heap as list of (support, itemset) tuples"""
        return sorted(self.heap, key=lambda x: -x[0])  # Sort descending by support

    def size(self) -> int:
        """Get current heap size"""
        return len(self.heap)

    def contains(self, itemset: Tuple) -> bool:
        """Check if itemset is in heap"""
        return itemset in self.itemset_map
