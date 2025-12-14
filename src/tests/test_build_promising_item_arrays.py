import pytest
from unittest.mock import Mock
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm
from ptf.min_heap import MinHeapTopK


class TestBuildPromisingItemArrays:
    """Test suite for build_promissing_item_arrays method."""

    def test_build_promising_item_arrays_basic(self):
        """Test build_promissing_item_arrays with basic input."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        # Create a mock min heap with some items
        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1,)),      # Single item
            (8, (1, 2)),     # Pair with item 1
            (6, (2, 3)),     # Pair with item 2
        ]

        all_items = [1, 2, 3]
        con_map = {frozenset([1]): 10, frozenset([1, 2])
                             : 8, frozenset([2, 3]): 6}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Verify the result is a dictionary
        assert isinstance(result, dict)
        assert set(result.keys()) == {1, 2, 3}

    def test_build_promising_item_arrays_empty_heap(self):
        """Test with an empty heap."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = []

        all_items = [1, 2, 3]
        con_map = {}
        rmsup = 1

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # All items should have empty promising items when heap is empty
        assert result[1] == []
        assert result[2] == []
        assert result[3] == []

    def test_build_promising_item_arrays_single_items_only(self):
        """Test with only single items in the heap."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1,)),
            (8, (2,)),
            (6, (3,)),
        ]

        all_items = [1, 2, 3]
        con_map = {frozenset([1]): 10, frozenset([2]): 8, frozenset([3]): 6}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Single items should be added to their own promising list
        assert 1 in result[1]
        assert 2 in result[2]
        assert 3 in result[3]

    def test_build_promising_item_arrays_pairs_only(self):
        """Test with only pair itemsets in the heap."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1, 2)),
            (8, (2, 3)),
            (6, (1, 3)),
        ]

        all_items = [1, 2, 3]

        con_map = {frozenset([1, 2]): 10, frozenset(
            [2, 3]): 8, frozenset([1, 3]): 6}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Item 1 should have items 2 and 3 as promising
        assert 2 in result[1]
        assert 3 in result[1]

        # Item 2 should have item 3 as promising
        assert 3 in result[2]

        # Item 3 should be empty (no pairs starting with 3)
        assert result[3] == []

    def test_build_promising_item_arrays_mixed_items(self):
        """Test with mixed single and pair itemsets."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=5)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1,)),
            (9, (1, 2)),
            (8, (1, 3)),
            (7, (2,)),
            (6, (2, 3)),
        ]

        all_items = [1, 2, 3]

        con_map = {
            frozenset([1]): 10, frozenset([1, 2]): 9, frozenset([1, 3]): 8,
            frozenset([2]): 7, frozenset([2, 3]): 6
        }
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Item 1 should have itself and items 2, 3
        assert 1 in result[1]
        assert 2 in result[1]
        assert 3 in result[1]

        # Item 2 should have itself and item 3
        assert 2 in result[2]
        assert 3 in result[2]

    def test_build_promising_item_arrays_duplicate_pairs(self):
        """Test when same pair appears multiple times."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1, 2)),
            (8, (1, 2)),  # Duplicate pair with different support
        ]

        all_items = [1, 2, 3]

        con_map = {frozenset([1, 2]): 10}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Item 1 should have item 2 (may appear twice if not deduplicated)
        assert 2 in result[1]

    def test_build_promising_item_arrays_large_itemsets(self):
        """Test with larger itemset (should be ignored, only 1 and 2 element sets)."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1, 2, 3)),  # 3-element itemset, should be ignored
            (9, (1, 2)),
            (8, (2,)),
        ]

        all_items = [1, 2, 3]

        con_map = {frozenset([1, 2]): 9, frozenset([2]): 8}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Only single and 2-element itemsets should be processed
        assert 2 in result[1]  # From (1,2) pair
        assert 2 in result[2]  # From single (2,)

    def test_build_promising_item_arrays_return_type(self):
        """Test that the return type is correct."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1, 2)),
        ]

        all_items = [1, 2, 3]

        con_map = {frozenset([1, 2]): 10}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Verify return type is dict with list values
        assert isinstance(result, dict)
        for key, value in result.items():
            assert isinstance(key, int)
            assert isinstance(value, list)
            assert all(isinstance(item, int) for item in value)

    def test_build_promising_item_arrays_all_items_initialized(self):
        """Test that all items in all_items are initialized in result."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (10, (1, 2)),
        ]

        all_items = [1, 2, 3, 4, 5]

        con_map = {frozenset([1, 2]): 10}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # All items should be present in result
        for item in all_items:
            assert item in result
            assert isinstance(result[item], list)

    def test_build_promising_item_arrays_high_support_values(self):
        """Test with high support values."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        mh = Mock(spec=MinHeapTopK)
        mh.get_all.return_value = [
            (1000, (1,)),
            (950, (1, 2)),
            (900, (2, 3)),
        ]

        all_items = [1, 2, 3]

        con_map = {frozenset([1]): 1000, frozenset(
            [1, 2]): 950, frozenset([2, 3]): 900}
        rmsup = 500

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        assert isinstance(result, dict)
        assert 1 in result[1]
        assert 2 in result[1]
        assert 3 in result[2]

    def test_build_promising_item_arrays_with_real_heap(self):
        """Test with a real MinHeapTopK instance."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        # Create a real MinHeapTopK and populate it
        mh = MinHeapTopK(k=3)
        mh.insert(support=10, itemset=(1,))
        mh.insert(support=9, itemset=(1, 2))
        mh.insert(support=8, itemset=(2,))

        all_items = [1, 2, 3]

        con_map = {frozenset([1]): 10, frozenset([1, 2]): 9, frozenset([2]): 8}
        rmsup = 5

        result = algo.build_promissing_item_arrays(
            mh, all_items, con_map, rmsup)

        # Verify result structure
        assert isinstance(result, dict)
        assert all(item in result for item in all_items)
        assert 1 in result[1]  # From single (1,)
        assert 2 in result[1]  # From pair (1, 2)
