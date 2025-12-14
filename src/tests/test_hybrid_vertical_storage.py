"""
Unit tests for Hybrid Vertical Storage Mode.

Tests cover:
- TidSetEntry creation and format selection
- All 6 intersection cases
- HybridVerticalIndex building and statistics
- Memory optimization vs naive tid-list approach
"""

import pytest
from ptf.hybrid_vertical_storage import (
    TidSetEntry,
    HybridTidSetIntersection,
    HybridVerticalIndex,
)


class TestTidSetEntry:
    """Tests for TidSetEntry class."""
    
    def test_create_tidlist_entry(self):
        """Test creating a tid-list entry."""
        tids = [1, 3, 5, 7]
        entry = TidSetEntry(
            item=2,
            flag=TidSetEntry.TID_LIST,
            data=tids,
            size_bytes=16,
            ni=10
        )
        
        assert entry.item == 2
        assert entry.flag == TidSetEntry.TID_LIST
        assert entry.get_support() == 4
        assert entry.get_tids() == tids
        assert entry.get_format_name() == "tid-list"
    
    def test_create_diflist_entry(self):
        """Test creating a dif-list entry."""
        dif = [0, 2, 4, 6, 8]  # Items NOT in set
        entry = TidSetEntry(
            item=3,
            flag=TidSetEntry.DIF_LIST,
            data=dif,
            size_bytes=20,
            ni=10
        )
        
        assert entry.get_support() == 5  # 10 - 5
        assert entry.get_tids() == [1, 3, 5, 7, 9]
        assert entry.get_format_name() == "dif-list"
    
    def test_create_bitvector_entry(self):
        """Test creating a bit-vector entry."""
        # Create bit-vector with bits 1, 3, 5, 7 set
        bv = bytes([0b10101010])  # Bits 1,3,5,7 in 8-bit number
        entry = TidSetEntry(
            item=4,
            flag=TidSetEntry.BIT_VECTOR,
            data=bv,
            size_bytes=1,
            ni=8
        )
        
        assert entry.get_support() == 4
        assert entry.get_tids() == [1, 3, 5, 7]
        assert entry.get_format_name() == "bit-vector"
    
    def test_invalid_flag(self):
        """Test that invalid flag raises error."""
        with pytest.raises(ValueError):
            TidSetEntry(
                item=1,
                flag=99,
                data=[1, 2, 3],
                size_bytes=12,
                ni=10
            )
    
    def test_create_optimal_entry_low_support(self):
        """Test optimal format selection with low support (tid-list)."""
        tids = [0, 2, 4]  # 3 out of 100
        entry = TidSetEntry._create_optimal_entry(item=1, tids=tids, ni=100)
        
        assert entry.flag == TidSetEntry.TID_LIST
        assert entry.get_support() == 3
    
    def test_create_optimal_entry_high_support(self):
        """Test optimal format selection with high support (dif-list preferred)."""
        # Create 95 items out of 100: sz_tid=380, sz_dif=20, sz_bv=13
        # Bit-vector (13) is actually smaller, so we get that instead of dif-list
        # Use 99 items to ensure dif-list wins: sz_tid=396, sz_dif=4, sz_bv=13
        tids = list(range(99))  # 99 out of 100
        entry = TidSetEntry._create_optimal_entry(item=2, tids=tids, ni=100)
        
        assert entry.flag == TidSetEntry.DIF_LIST
        assert entry.get_support() == 99
    
    def test_create_optimal_entry_medium_support(self):
        """Test optimal format selection with medium support (bit-vector)."""
        tids = list(range(0, 50, 2))  # 25 out of 100
        entry = TidSetEntry._create_optimal_entry(item=3, tids=tids, ni=100)
        
        assert entry.flag == TidSetEntry.BIT_VECTOR
        assert entry.get_support() == 25
    
    def test_bit_count_lookup_table(self):
        """Test bit counting with lookup table."""
        # 0b11111111 = 255, should have 8 bits set
        bv = bytes([255, 0, 127])  # 8 + 0 + 7 = 15 bits
        count = TidSetEntry._count_bits_in_bytes(bv)
        assert count == 15
    
    def test_create_bitvector_from_tids(self):
        """Test creating bit-vector from tid list."""
        tids = [0, 2, 5, 7]
        bv = TidSetEntry._create_bitvector(tids, ni=16)
        
        # Expected: byte 0 has bits 0,2,5,7 set
        assert bv[0] == 0b10100101  # bits 0,2,5,7
        assert bv[1] == 0b00000000
    
    def test_get_tids_from_diflist(self):
        """Test decoding tids from dif-list."""
        dif = [1, 3, 4]  # Exclude tids 1, 3, 4
        entry = TidSetEntry(
            item=5,
            flag=TidSetEntry.DIF_LIST,
            data=dif,
            size_bytes=12,
            ni=6
        )
        
        tids = entry.get_tids()
        assert tids == [0, 2, 5]


class TestIntersectionCases:
    """Tests for all 6 tid-set intersection cases."""
    
    def test_case1_tidlist_tidlist(self):
        """Case 1: Tid-list ∩ Tid-list"""
        L1 = [1, 3, 5, 7]
        L2 = [3, 5, 9, 11]
        result = HybridTidSetIntersection.intersect_tidlist_tidlist(L1, L2)
        
        assert result == [3, 5]
    
    def test_case1_tidlist_tidlist_empty(self):
        """Case 1: Tid-list ∩ Tid-list with no intersection."""
        L1 = [1, 2, 3]
        L2 = [4, 5, 6]
        result = HybridTidSetIntersection.intersect_tidlist_tidlist(L1, L2)
        
        assert result == []
    
    def test_case1_tidlist_tidlist_one_empty(self):
        """Case 1: Tid-list ∩ Tid-list with one empty list."""
        L1 = [1, 2, 3]
        L2 = []
        result = HybridTidSetIntersection.intersect_tidlist_tidlist(L1, L2)
        
        assert result == []
    
    def test_case2_diflist_diflist(self):
        """Case 2: Dif-list ∩ Dif-list"""
        # D1 = {0,2,4}, L1 = {1,3,5,6,7,8,9}
        # D2 = {1,3,5}, L2 = {0,2,4,6,7,8,9}
        # L1 ∩ L2 = {6,7,8,9}
        D1 = [0, 2, 4]
        D2 = [1, 3, 5]
        result = HybridTidSetIntersection.intersect_diflist_diflist(D1, D2, ni=10)
        
        assert result == [6, 7, 8, 9]
    
    def test_case3_bitvector_bitvector(self):
        """Case 3: Bit-vector ∩ Bit-vector"""
        BV1 = bytes([0b10101010])  # Bits 1,3,5,7
        BV2 = bytes([0b11110000])  # Bits 4,5,6,7
        result = HybridTidSetIntersection.intersect_bitvector_bitvector(BV1, BV2)
        
        expected = bytes([0b10100000])  # Bits 5,7
        assert result == expected
    
    def test_case3_bitvector_bitvector_multiple_bytes(self):
        """Case 3: Bit-vector with multiple bytes."""
        BV1 = bytes([0xFF, 0x00])
        BV2 = bytes([0x0F, 0xF0])
        result = HybridTidSetIntersection.intersect_bitvector_bitvector(BV1, BV2)
        
        assert result == bytes([0x0F, 0x00])
    
    def test_case4_tidlist_diflist(self):
        """Case 4: Tid-list ∩ Dif-list"""
        # L = {1,3,5,7,9}, D = {1,5,9} (exclude 1,5,9 from full set)
        # Result = L \ D = {3,7}
        L = [1, 3, 5, 7, 9]
        D = [1, 5, 9]
        result = HybridTidSetIntersection.intersect_tidlist_diflist(L, D)
        
        assert result == [3, 7]
    
    def test_case4_tidlist_diflist_no_intersection(self):
        """Case 4: Tid-list ∩ Dif-list with no intersection."""
        L = [1, 5, 9]
        D = [1, 5, 9]
        result = HybridTidSetIntersection.intersect_tidlist_diflist(L, D)
        
        assert result == []
    
    def test_case5_tidlist_bitvector(self):
        """Case 5: Tid-list ∩ Bit-vector"""
        L = [1, 3, 5, 7]
        BV = bytes([0b10101010])  # Bits 1,3,5,7
        result = HybridTidSetIntersection.intersect_tidlist_bitvector(L, BV)
        
        assert result == [1, 3, 5, 7]
    
    def test_case5_tidlist_bitvector_partial(self):
        """Case 5: Tid-list ∩ Bit-vector with partial overlap."""
        L = [1, 3, 5, 7]
        BV = bytes([0b00001010])  # Bits 1,3
        result = HybridTidSetIntersection.intersect_tidlist_bitvector(L, BV)
        
        assert result == [1, 3]
    
    def test_case6_diflist_bitvector(self):
        """Case 6: Dif-list ∩ Bit-vector"""
        # D = {0,2,4} (exclude 0,2,4), BV has bits 1,3,5,7 set
        D = [0, 2, 4]
        BV = bytes([0b10101010])
        result = HybridTidSetIntersection.intersect_diflist_bitvector(D, BV, ni=8)
        
        expected = bytes([0b10101010])  # Same, since excluded tids aren't in BV anyway
        assert result == expected
    
    def test_case6_diflist_bitvector_clears_bits(self):
        """Case 6: Dif-list ∩ Bit-vector clears excluded bits."""
        # D = {1,3}, BV = 0xFF (all bits set)
        # Clear bits 1,3: 0xFF & ~0b00001010 = 0xFF & 0xF5 = 0xF5
        D = [1, 3]
        BV = bytes([0xFF])
        result = HybridTidSetIntersection.intersect_diflist_bitvector(D, BV, ni=8)
        
        expected = bytes([0b11110101])  # Clear bits 1,3
        assert result == expected


class TestHybridVerticalIndex:
    """Tests for HybridVerticalIndex."""
    
    def test_build_simple_partition(self):
        """Test building index from simple partition."""
        partition_data = [
            [1, 2],
            [1, 3],
            [1, 2, 3],
            [1, 4],
        ]
        
        index = HybridVerticalIndex(partition_item=1, ni=4)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3, 4]
        )
        
        # Check item 1 (prefix): all tids
        assert index.get_support(1) == 4
        
        # Check item 2: tids 0, 2
        assert index.get_support(2) == 2
        
        # Check item 3: tids 1, 2
        assert index.get_support(3) == 2
        
        # Check item 4: tid 3
        assert index.get_support(4) == 1
    
    def test_format_selection(self):
        """Test format selection in hybrid index."""
        # Create partition: item 1 appears in all 50 transactions
        # sz_tid=200, sz_dif=0, sz_bv=7 -> bit-vector wins
        partition_data = [[1, i] for i in range(2, 52)]  # 50 transactions, all have item 1
        
        index = HybridVerticalIndex(partition_item=1, ni=50)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=list(range(1, 52))
        )
        
        # Item 1 is in all transactions (sz_dif=0 makes dif-list best, or bit-vector)
        fmt = index.get_format(1)
        assert fmt in [TidSetEntry.DIF_LIST, TidSetEntry.BIT_VECTOR]
    
    def test_get_entry(self):
        """Test getting entry from index."""
        partition_data = [
            [1, 2],
            [1, 3],
        ]
        
        index = HybridVerticalIndex(partition_item=1, ni=2)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3]
        )
        
        entry = index.get_entry(2)
        assert entry.item == 2
        assert entry.get_tids() == [0]
    
    def test_contains_item(self):
        """Test checking if item is in index."""
        partition_data = [[1, 2]]
        
        index = HybridVerticalIndex(partition_item=1, ni=1)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3]
        )
        
        assert index.contains_item(1)
        assert index.contains_item(2)
        # Item 3 is in promising_items but not in any transaction
        # So it's added with support=0 (empty tid-list)
        # The index still contains it (with zero support)
        assert index.contains_item(3)
        assert index.get_support(3) == 0
    
    def test_get_items(self):
        """Test getting list of items in index."""
        partition_data = [[1, 2, 3]]
        
        index = HybridVerticalIndex(partition_item=1, ni=1)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3, 4]
        )
        
        items = sorted(index.get_items())
        # Item 4 not in partition, so shouldn't be in index (or has support 0)
        assert 1 in items
        assert 2 in items
        assert 3 in items
    
    def test_statistics(self):
        """Test format distribution statistics."""
        partition_data = [
            [1, 2, 3, 4, 5],
            [1, 3, 5],
            [1, 5],
        ]
        
        index = HybridVerticalIndex(partition_item=1, ni=3)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3, 4, 5]
        )
        
        stats = index.get_stats()
        assert stats['ni'] == 3
        assert stats['total_items'] == 5
        assert stats['total_bytes'] > 0
    
    def test_memory_comparison(self):
        """Test memory comparison with naive tid-list approach."""
        partition_data = [
            [1, 2, 3],
            [1, 3, 5],
            [1, 5, 7],
            [1, 7, 9],
        ]
        
        index = HybridVerticalIndex(partition_item=1, ni=4)
        index.build_from_partition(
            partition_data=partition_data,
            promising_items=[1, 2, 3, 5, 7, 9]
        )
        
        naive_bytes, reduction = index.memory_comparison()
        
        # Should have some reduction with bit-vectors
        assert naive_bytes > 0
        assert reduction >= 1.0


class TestIntersectionDispatcher:
    """Tests for intersection dispatcher with various format combinations."""
    
    def test_intersect_tidlist_tidlist(self):
        """Test dispatcher with tidlist-tidlist case."""
        entry1 = TidSetEntry(1, TidSetEntry.TID_LIST, [1, 3, 5], 12, 10)
        entry2 = TidSetEntry(2, TidSetEntry.TID_LIST, [3, 5, 7], 12, 10)
        
        result, support = HybridTidSetIntersection.intersect(entry1, entry2, ni=10)
        
        assert support == 2
        assert result.get_tids() == [3, 5]
    
    def test_intersect_bitvector_bitvector(self):
        """Test dispatcher with bitvector-bitvector case."""
        bv1 = bytes([0b10101010])
        bv2 = bytes([0b11110000])
        
        entry1 = TidSetEntry(1, TidSetEntry.BIT_VECTOR, bv1, 1, 8)
        entry2 = TidSetEntry(2, TidSetEntry.BIT_VECTOR, bv2, 1, 8)
        
        result, support = HybridTidSetIntersection.intersect(entry1, entry2, ni=8)
        
        assert support == 2  # Bits 5,7
    
    def test_intersect_mixed_formats(self):
        """Test dispatcher with mixed format combination."""
        entry1 = TidSetEntry(1, TidSetEntry.TID_LIST, [1, 3, 5, 7], 16, 10)
        bv = bytes([0b10101010])
        entry2 = TidSetEntry(2, TidSetEntry.BIT_VECTOR, bv, 1, 8)
        
        result, support = HybridTidSetIntersection.intersect(entry1, entry2, ni=8)
        
        assert support == 4  # All overlap


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
