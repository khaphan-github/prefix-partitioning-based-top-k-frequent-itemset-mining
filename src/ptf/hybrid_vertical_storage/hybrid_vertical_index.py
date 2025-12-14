"""
HybridVerticalIndex: Manages all tid-sets for a partition with hybrid storage.

Builds vertical representation from raw transaction data and stores each item's
tid-set in the optimal format (tid-list, dif-list, or bit-vector).
"""

from typing import List, Dict, Tuple
from .tid_set_entry import TidSetEntry


class HybridVerticalIndex:
    """
    Vertical index for a partition with hybrid tid-set storage.
    
    Attributes:
        partition_item (int): Prefix item xi
        ni (int): Number of transactions in partition
        entries (Dict[int, TidSetEntry]): item -> TidSetEntry mapping
        stats (Dict): Format usage statistics (count of each format)
    """
    
    def __init__(self, partition_item: int, ni: int):
        """
        Initialize hybrid vertical index.
        
        Args:
            partition_item: Prefix item xi
            ni: Number of transactions
        """
        self.partition_item = partition_item
        self.ni = ni
        self.entries: Dict[int, TidSetEntry] = {}
        self.stats = {
            'tidlist_count': 0,
            'diflist_count': 0,
            'bitvector_count': 0,
            'tidlist_bytes': 0,
            'diflist_bytes': 0,
            'bitvector_bytes': 0,
        }
    
    def build_from_partition(
        self,
        partition_data: List[List[int]],
        promising_items: List[int]
    ) -> None:
        """
        Build vertical index from raw transaction data.
        
        For each promising item, collect its tid-set and choose optimal format.
        Prefix item xi is stored with tid-set = all tids (support = ni).
        
        Args:
            partition_data: Transactions in partition (List[List[int]])
            promising_items: Promising items for this partition (includes partition_item)
        """
        # Step 1: Collect tid-sets for all items
        tidsets: Dict[int, List[int]] = {}
        
        # Initialize empty tid-sets for all promising items except partition_item
        for item in promising_items:
            if item != self.partition_item:
                tidsets[item] = []
        
        # Prefix item appears in all transactions
        tidsets[self.partition_item] = list(range(self.ni))
        
        # Step 2: Assign local TID and build tid-sets
        for local_tid, transaction in enumerate(partition_data):
            for item in transaction:
                if item in tidsets and item != self.partition_item:
                    tidsets[item].append(local_tid)
        
        # Step 3: Sort tid-sets for efficient operations
        for item in tidsets:
            tidsets[item].sort()
        
        # Step 4: Create TidSetEntry with optimal format for each item
        for item, tids in tidsets.items():
            entry = TidSetEntry._create_optimal_entry(
                item=item,
                tids=tids,
                ni=self.ni
            )
            self.entries[item] = entry
            
            # Update statistics
            if entry.flag == TidSetEntry.TID_LIST:
                self.stats['tidlist_count'] += 1
                self.stats['tidlist_bytes'] += entry.size_bytes
            elif entry.flag == TidSetEntry.DIF_LIST:
                self.stats['diflist_count'] += 1
                self.stats['diflist_bytes'] += entry.size_bytes
            elif entry.flag == TidSetEntry.BIT_VECTOR:
                self.stats['bitvector_count'] += 1
                self.stats['bitvector_bytes'] += entry.size_bytes
    
    def get_entry(self, item: int) -> TidSetEntry:
        """
        Get tid-set entry for an item.
        
        Args:
            item: Item identifier
        
        Returns:
            TidSetEntry for the item
        
        Raises:
            KeyError: If item not in index
        """
        return self.entries[item]
    
    def get_support(self, item: int) -> int:
        """
        Get support of an item (without full decoding for dif-list/bit-vector).
        
        Args:
            item: Item identifier
        
        Returns:
            Support value
        """
        if item not in self.entries:
            return 0
        return self.entries[item].get_support()
    
    def get_format(self, item: int) -> int:
        """
        Get format flag of an item's tid-set.
        
        Args:
            item: Item identifier
        
        Returns:
            Format flag (TidSetEntry.TID_LIST, DIF_LIST, or BIT_VECTOR)
        """
        if item not in self.entries:
            return None
        return self.entries[item].flag
    
    def get_format_name(self, item: int) -> str:
        """
        Get human-readable format name for an item.
        
        Args:
            item: Item identifier
        
        Returns:
            Format name ("tid-list", "dif-list", or "bit-vector")
        """
        if item not in self.entries:
            return "unknown"
        return self.entries[item].get_format_name()
    
    def contains_item(self, item: int) -> bool:
        """Check if item is in index."""
        return item in self.entries
    
    def get_items(self) -> List[int]:
        """Get list of all items in index."""
        return list(self.entries.keys())
    
    def get_stats(self) -> Dict:
        """
        Get format distribution statistics.
        
        Returns:
            Dictionary with format counts and memory usage
        """
        total_items = sum([
            self.stats['tidlist_count'],
            self.stats['diflist_count'],
            self.stats['bitvector_count']
        ])
        
        total_bytes = sum([
            self.stats['tidlist_bytes'],
            self.stats['diflist_bytes'],
            self.stats['bitvector_bytes']
        ])
        
        return {
            'partition_item': self.partition_item,
            'ni': self.ni,
            'total_items': total_items,
            'total_bytes': total_bytes,
            'tidlist_count': self.stats['tidlist_count'],
            'tidlist_bytes': self.stats['tidlist_bytes'],
            'diflist_count': self.stats['diflist_count'],
            'diflist_bytes': self.stats['diflist_bytes'],
            'bitvector_count': self.stats['bitvector_count'],
            'bitvector_bytes': self.stats['bitvector_bytes'],
        }
    
    def print_stats(self) -> None:
        """Print format distribution statistics."""
        stats = self.get_stats()
        print(f"\n=== Hybrid Vertical Index Stats ===")
        print(f"Partition item: {stats['partition_item']}")
        print(f"Transactions: {stats['ni']}")
        print(f"Total items: {stats['total_items']}")
        print(f"Total memory: {stats['total_bytes']:,} bytes")
        print(f"\nFormat distribution:")
        print(f"  Tid-list:   {stats['tidlist_count']:3d} items, {stats['tidlist_bytes']:10,} bytes")
        print(f"  Dif-list:   {stats['diflist_count']:3d} items, {stats['diflist_bytes']:10,} bytes")
        print(f"  Bit-vector: {stats['bitvector_count']:3d} items, {stats['bitvector_bytes']:10,} bytes")
    
    def memory_comparison(self) -> Tuple[int, float]:
        """
        Compare memory with naive tid-list approach.
        
        Returns:
            Tuple of (naive_bytes, reduction_factor)
        """
        naive_bytes = 0
        for entry in self.entries.values():
            support = entry.get_support()
            naive_bytes += 4 * support
        
        actual_bytes = self.get_stats()['total_bytes']
        
        if naive_bytes == 0:
            return 0, 1.0
        
        reduction_factor = naive_bytes / actual_bytes if actual_bytes > 0 else float('inf')
        return naive_bytes, reduction_factor
