"""
TidSetEntry: Represents a single tid-set with hybrid format storage.

A tid-set for item xj can be stored in one of 3 formats:
- flag=1: tid-list (sorted list of transaction IDs containing xj)
- flag=0: dif-list (sorted list of transaction IDs NOT containing xj)
- flag=-1: bit-vector (byte array where bit i = 1 if tid i contains xj)
"""

from typing import List, Union
import struct


class TidSetEntry:
    """
    Represents a tid-set with format optimization.
    
    Attributes:
        item (int): Item identifier (xj)
        flag (int): Format indicator (1=tid-list, 0=dif-list, -1=bit-vector)
        data (Union[List[int], bytes]): Encoded tid-set
        size_bytes (int): Memory footprint in bytes
        ni (int): Total number of transactions in partition (for dif-list/bit-vector)
    """
    
    # Format constants
    TID_LIST = 1
    DIF_LIST = 0
    BIT_VECTOR = -1
    
    def __init__(
        self,
        item: int,
        flag: int,
        data: Union[List[int], bytes],
        size_bytes: int,
        ni: int = 0
    ):
        """
        Initialize a TidSetEntry.
        
        Args:
            item: Item identifier
            flag: Format (TidSetEntry.TID_LIST, DIF_LIST, or BIT_VECTOR)
            data: Encoded tid-set (List[int] for lists, bytes for bit-vector)
            size_bytes: Size in bytes for memory tracking
            ni: Total transactions (required for dif-list/bit-vector support calculation)
        """
        if flag not in (self.TID_LIST, self.DIF_LIST, self.BIT_VECTOR):
            raise ValueError(f"Invalid flag: {flag}. Must be {self.TID_LIST}, {self.DIF_LIST}, or {self.BIT_VECTOR}")
        
        self.item = item
        self.flag = flag
        self.data = data
        self.size_bytes = size_bytes
        self.ni = ni
    
    def get_support(self) -> int:
        """
        Get support (number of transactions containing item) without full decoding.
        
        Returns:
            Support value for this tid-set
        """
        if self.flag == self.TID_LIST:
            # Support = number of tids in the list
            return len(self.data) if isinstance(self.data, list) else 0
        
        elif self.flag == self.DIF_LIST:
            # Support = total transactions - complement size
            return self.ni - (len(self.data) if isinstance(self.data, list) else 0)
        
        elif self.flag == self.BIT_VECTOR:
            # Support = number of 1-bits in bit-vector
            if isinstance(self.data, bytes):
                return self._count_bits_in_bytes(self.data)
            return 0
        
        return 0
    
    def get_tids(self) -> List[int]:
        """
        Decode and return actual tid list.
        
        For tid-list: return as-is
        For dif-list: compute complement (all tids not in dif-list)
        For bit-vector: extract tids where bit=1
        
        Returns:
            Sorted list of transaction IDs containing this item
        """
        if self.flag == self.TID_LIST:
            return list(self.data) if isinstance(self.data, list) else []
        
        elif self.flag == self.DIF_LIST:
            # Complement: all tids not in dif-list
            if isinstance(self.data, list):
                dif_set = set(self.data)
                return sorted([tid for tid in range(self.ni) if tid not in dif_set])
            return list(range(self.ni))
        
        elif self.flag == self.BIT_VECTOR:
            # Extract tids from bit-vector
            if isinstance(self.data, bytes):
                tids = []
                for byte_idx, byte_val in enumerate(self.data):
                    for bit_idx in range(8):
                        tid = byte_idx * 8 + bit_idx
                        if tid < self.ni and (byte_val & (1 << bit_idx)):
                            tids.append(tid)
                return tids
            return []
        
        return []
    
    def get_size(self) -> int:
        """Get memory size in bytes."""
        return self.size_bytes
    
    def get_format_name(self) -> str:
        """Get human-readable format name."""
        if self.flag == self.TID_LIST:
            return "tid-list"
        elif self.flag == self.DIF_LIST:
            return "dif-list"
        elif self.flag == self.BIT_VECTOR:
            return "bit-vector"
        return "unknown"
    
    def __repr__(self) -> str:
        support = self.get_support()
        return (
            f"TidSetEntry(item={self.item}, format={self.get_format_name()}, "
            f"support={support}, size={self.size_bytes}B)"
        )
    
    @staticmethod
    def _count_bits_in_bytes(data: bytes) -> int:
        """
        Count number of 1-bits in byte array using lookup table.
        
        More efficient than counting bit-by-bit.
        """
        # Lookup table: number of 1-bits for each byte value 0-255
        BIT_COUNT = [
            0, 1, 1, 2, 1, 2, 2, 3, 1, 2, 2, 3, 2, 3, 3, 4,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            1, 2, 2, 3, 2, 3, 3, 4, 2, 3, 3, 4, 3, 4, 4, 5,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            2, 3, 3, 4, 3, 4, 4, 5, 3, 4, 4, 5, 4, 5, 5, 6,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            3, 4, 4, 5, 4, 5, 5, 6, 4, 5, 5, 6, 5, 6, 6, 7,
            4, 5, 5, 6, 5, 6, 6, 7, 5, 6, 6, 7, 6, 7, 7, 8,
        ]
        return sum(BIT_COUNT[byte_val] for byte_val in data)
    
    @staticmethod
    def _create_optimal_entry(item: int, tids: List[int], ni: int) -> 'TidSetEntry':
        """
        Create TidSetEntry with optimal format for given tids.
        
        Choose format based on memory size:
        - Tid-list if support is low
        - Dif-list if support is high
        - Bit-vector if support is medium
        """
        L = len(tids)
        sz_tid = 4 * L
        sz_dif = 4 * (ni - L)
        sz_bv = (ni + 7) // 8  # Ceiling division
        
        if sz_tid <= sz_dif and sz_tid <= sz_bv:
            # Tid-list is smallest
            return TidSetEntry(
                item=item,
                flag=TidSetEntry.TID_LIST,
                data=tids,
                size_bytes=sz_tid,
                ni=ni
            )
        elif sz_dif <= sz_tid and sz_dif <= sz_bv:
            # Dif-list is smallest
            tid_set = set(tids)
            dif_list = [tid for tid in range(ni) if tid not in tid_set]
            return TidSetEntry(
                item=item,
                flag=TidSetEntry.DIF_LIST,
                data=dif_list,
                size_bytes=sz_dif,
                ni=ni
            )
        else:
            # Bit-vector is smallest
            bv = TidSetEntry._create_bitvector(tids, ni)
            return TidSetEntry(
                item=item,
                flag=TidSetEntry.BIT_VECTOR,
                data=bv,
                size_bytes=sz_bv,
                ni=ni
            )
    
    @staticmethod
    def _create_bitvector_entry(item: int, bv: bytes, ni: int) -> 'TidSetEntry':
        """Create a bit-vector TidSetEntry."""
        return TidSetEntry(
            item=item,
            flag=TidSetEntry.BIT_VECTOR,
            data=bv,
            size_bytes=len(bv),
            ni=ni
        )
    
    @staticmethod
    def _create_bitvector(tids: List[int], ni: int) -> bytes:
        """Create bit-vector from tid list."""
        num_bytes = (ni + 7) // 8
        bv = bytearray(num_bytes)
        
        for tid in tids:
            byte_idx = tid // 8
            bit_idx = tid % 8
            if byte_idx < num_bytes:
                bv[byte_idx] |= (1 << bit_idx)
        
        return bytes(bv)
