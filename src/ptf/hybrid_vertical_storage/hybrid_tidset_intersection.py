"""
HybridTidSetIntersection: Implements 6 cases of tid-set intersection with hybrid formats.

Cases:
1. Tid-list ∩ Tid-list → Tid-list (merge two sorted lists)
2. Dif-list ∩ Dif-list → Support via union complement
3. Bit-vector ∩ Bit-vector → Bit-vector (bitwise AND)
4. Tid-list ∩ Dif-list → Tid-list (set difference)
5. Tid-list ∩ Bit-vector → Tid-list (filter by bits)
6. Dif-list ∩ Bit-vector → Bit-vector (clear bits in complement)
"""

from typing import List, Tuple
from .tid_set_entry import TidSetEntry


class HybridTidSetIntersection:
    """
    Intersection dispatcher for hybrid tid-set formats.
    """
    
    @staticmethod
    def intersect(
        entry1: TidSetEntry,
        entry2: TidSetEntry,
        ni: int
    ) -> Tuple[TidSetEntry, int]:
        """
        Intersect two tid-set entries with automatic case handling.
        
        Args:
            entry1: First tid-set entry
            entry2: Second tid-set entry
            ni: Total number of transactions in partition
        
        Returns:
            Tuple of (result_entry, support) where result_entry is a TidSetEntry
            with the intersection result, and support is the number of transactions.
        """
        flag1 = entry1.flag
        flag2 = entry2.flag
        
        # Determine which case based on flags
        if flag1 == TidSetEntry.TID_LIST and flag2 == TidSetEntry.TID_LIST:
            # Case 1: Tid-list ∩ Tid-list
            result_tids = HybridTidSetIntersection.intersect_tidlist_tidlist(
                entry1.data, entry2.data
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,  # Placeholder, will be set by caller
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.DIF_LIST and flag2 == TidSetEntry.DIF_LIST:
            # Case 2: Dif-list ∩ Dif-list
            result_tids = HybridTidSetIntersection.intersect_diflist_diflist(
                entry1.data, entry2.data, ni
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.BIT_VECTOR and flag2 == TidSetEntry.BIT_VECTOR:
            # Case 3: Bit-vector ∩ Bit-vector
            result_bv = HybridTidSetIntersection.intersect_bitvector_bitvector(
                entry1.data, entry2.data
            )
            support = TidSetEntry._count_bits_in_bytes(result_bv)
            return TidSetEntry._create_bitvector_entry(item=-1, bv=result_bv, ni=ni), support
        
        elif flag1 == TidSetEntry.TID_LIST and flag2 == TidSetEntry.DIF_LIST:
            # Case 4: Tid-list ∩ Dif-list
            result_tids = HybridTidSetIntersection.intersect_tidlist_diflist(
                entry1.data, entry2.data
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.DIF_LIST and flag2 == TidSetEntry.TID_LIST:
            # Case 4 (reversed): Dif-list ∩ Tid-list
            result_tids = HybridTidSetIntersection.intersect_tidlist_diflist(
                entry2.data, entry1.data
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.TID_LIST and flag2 == TidSetEntry.BIT_VECTOR:
            # Case 5: Tid-list ∩ Bit-vector
            result_tids = HybridTidSetIntersection.intersect_tidlist_bitvector(
                entry1.data, entry2.data
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.BIT_VECTOR and flag2 == TidSetEntry.TID_LIST:
            # Case 5 (reversed): Bit-vector ∩ Tid-list
            result_tids = HybridTidSetIntersection.intersect_tidlist_bitvector(
                entry2.data, entry1.data
            )
            support = len(result_tids)
            return TidSetEntry._create_optimal_entry(
                item=-1,
                tids=result_tids,
                ni=ni
            ), support
        
        elif flag1 == TidSetEntry.DIF_LIST and flag2 == TidSetEntry.BIT_VECTOR:
            # Case 6: Dif-list ∩ Bit-vector
            result_bv = HybridTidSetIntersection.intersect_diflist_bitvector(
                entry1.data, entry2.data, ni
            )
            support = TidSetEntry._count_bits_in_bytes(result_bv)
            return TidSetEntry._create_bitvector_entry(item=-1, bv=result_bv, ni=ni), support
        
        elif flag1 == TidSetEntry.BIT_VECTOR and flag2 == TidSetEntry.DIF_LIST:
            # Case 6 (reversed): Bit-vector ∩ Dif-list
            result_bv = HybridTidSetIntersection.intersect_diflist_bitvector(
                entry2.data, entry1.data, ni
            )
            support = TidSetEntry._count_bits_in_bytes(result_bv)
            return TidSetEntry._create_bitvector_entry(item=-1, bv=result_bv, ni=ni), support
        
        else:
            raise ValueError(f"Unknown format combination: {flag1}, {flag2}")
    
    # ========== Case 1: Tid-list ∩ Tid-list ==========
    @staticmethod
    def intersect_tidlist_tidlist(L1: List[int], L2: List[int]) -> List[int]:
        """
        Intersect two tid-lists using two-pointer merge.
        
        Both lists must be sorted in ascending order.
        Time complexity: O(|L1| + |L2|)
        
        Args:
            L1: Sorted tid-list 1
            L2: Sorted tid-list 2
        
        Returns:
            Sorted intersection of L1 and L2
        """
        result = []
        i, j = 0, 0
        
        while i < len(L1) and j < len(L2):
            if L1[i] == L2[j]:
                result.append(L1[i])
                i += 1
                j += 1
            elif L1[i] < L2[j]:
                i += 1
            else:
                j += 1
        
        return result
    
    # ========== Case 2: Dif-list ∩ Dif-list ==========
    @staticmethod
    def intersect_diflist_diflist(D1: List[int], D2: List[int], ni: int) -> List[int]:
        """
        Intersect tid-sets represented as dif-lists.
        
        D1, D2 are complements: transactions NOT in original sets.
        L_intersect = {1..ni} \ (D1 ∪ D2)
        
        Args:
            D1: Sorted dif-list 1 (tid NOT in set 1)
            D2: Sorted dif-list 2 (tid NOT in set 2)
            ni: Total transactions
        
        Returns:
            Sorted tid-list of intersection
        """
        # Compute D1 ∪ D2 (union of exclusions)
        d_union = HybridTidSetIntersection._union_sorted_lists(D1, D2)
        
        # Compute complement: L = {1..ni} \ (D1 ∪ D2)
        d_union_set = set(d_union)
        result = [tid for tid in range(ni) if tid not in d_union_set]
        
        return result
    
    # ========== Case 3: Bit-vector ∩ Bit-vector ==========
    @staticmethod
    def intersect_bitvector_bitvector(BV1: bytes, BV2: bytes) -> bytes:
        """
        Intersect two bit-vectors using bitwise AND.
        
        Bit i = 1 in result iff bit i = 1 in both BV1 and BV2.
        Time complexity: O(bytes)
        
        Args:
            BV1: Bit-vector 1
            BV2: Bit-vector 2 (same size)
        
        Returns:
            Bit-vector result of BV1 & BV2
        """
        if len(BV1) != len(BV2):
            raise ValueError(f"Bit-vector size mismatch: {len(BV1)} vs {len(BV2)}")
        
        result = bytearray(len(BV1))
        for i in range(len(BV1)):
            result[i] = BV1[i] & BV2[i]
        
        return bytes(result)
    
    # ========== Case 4: Tid-list ∩ Dif-list ==========
    @staticmethod
    def intersect_tidlist_diflist(L: List[int], D: List[int]) -> List[int]:
        """
        Intersect tid-list with dif-list.
        
        L = transactions in set 1
        D = transactions NOT in set 2
        Result = L \ D (tids in L that are not in D)
        
        Args:
            L: Sorted tid-list
            D: Sorted dif-list (exclusions)
        
        Returns:
            Sorted intersection tid-list
        """
        result = []
        i, j = 0, 0
        
        while i < len(L):
            if j >= len(D):
                # All remaining L elements are in result
                result.extend(L[i:])
                break
            
            if L[i] == D[j]:
                # L[i] is excluded, skip it
                i += 1
                j += 1
            elif L[i] < D[j]:
                # L[i] is not in D, include it
                result.append(L[i])
                i += 1
            else:
                j += 1
        
        return result
    
    # ========== Case 5: Tid-list ∩ Bit-vector ==========
    @staticmethod
    def intersect_tidlist_bitvector(L: List[int], BV: bytes) -> List[int]:
        """
        Intersect tid-list with bit-vector.
        
        For each tid in L, check if corresponding bit in BV is 1.
        
        Args:
            L: Sorted tid-list
            BV: Bit-vector
        
        Returns:
            Sorted tid-list of intersection
        """
        result = []
        for tid in L:
            byte_idx = tid // 8
            bit_idx = tid % 8
            
            if byte_idx < len(BV):
                if BV[byte_idx] & (1 << bit_idx):
                    result.append(tid)
        
        return result
    
    # ========== Case 6: Dif-list ∩ Bit-vector ==========
    @staticmethod
    def intersect_diflist_bitvector(D: List[int], BV: bytes, ni: int) -> bytes:
        """
        Intersect dif-list with bit-vector.
        
        D = transactions NOT in set 1
        BV = transactions in set 2
        Result = transactions in set 2 but also in set 1
            = BV with bits cleared for tids in D
        
        Args:
            D: Sorted dif-list (exclusions from set 1)
            BV: Bit-vector for set 2
            ni: Total transactions
        
        Returns:
            Bit-vector result (BV with D tids cleared)
        """
        result = bytearray(BV)
        
        for tid in D:
            byte_idx = tid // 8
            bit_idx = tid % 8
            
            if byte_idx < len(result):
                # Clear the bit
                result[byte_idx] &= ~(1 << bit_idx)
        
        return bytes(result)
    
    # ========== Helper methods ==========
    @staticmethod
    def _union_sorted_lists(L1: List[int], L2: List[int]) -> List[int]:
        """
        Compute union of two sorted lists.
        
        Args:
            L1: Sorted list 1
            L2: Sorted list 2
        
        Returns:
            Sorted union of L1 and L2
        """
        result = []
        i, j = 0, 0
        
        while i < len(L1) and j < len(L2):
            if L1[i] == L2[j]:
                result.append(L1[i])
                i += 1
                j += 1
            elif L1[i] < L2[j]:
                result.append(L1[i])
                i += 1
            else:
                result.append(L2[j])
                j += 1
        
        # Add remaining elements
        result.extend(L1[i:])
        result.extend(L2[j:])
        
        return result
