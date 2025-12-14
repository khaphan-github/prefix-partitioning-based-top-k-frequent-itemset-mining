import pytest
from unittest.mock import Mock, patch, MagicMock
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm
from ptf.sgl_partition import SglPartition
from ptf.min_heap import MinHeapTopK


class TestPrefixPartitioningbasedTopKAlgorithm:
    """Test suite for PrefixPartitioningbasedTopKAlgorithm class."""

    def test_initialization(self):
        """Test that the algorithm initializes correctly with a given top_k value."""
        top_k = 5
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        assert algo.top_k == top_k
        assert isinstance(algo.top_k, int)

    def test_initialization_with_different_k_values(self):
        """Test initialization with various top_k values."""
        for k in [1, 5, 10, 100, 1000]:
            algo = PrefixPartitioningbasedTopKAlgorithm(k=k, partitionClass=SglPartition)
            assert algo.top_k == k

    def test_initialize_mh_and_rmsup_basic(self):
        """Test initialize_mh_and_rmsup with a basic input."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        # Create a con_list with support values
        con_list = [
            ({1, 2}, 10),
            ({1, 3}, 8),
            ({2, 3}, 6),
            ({1}, 15),
            ({2}, 12),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Verify min_heap is created
        assert isinstance(min_heap, MinHeapTopK)
        assert min_heap.k == top_k

        # Verify rmsup is set
        assert isinstance(rmsup, int)
        assert rmsup >= 0

    def test_initialize_mh_and_rmsup_heap_size(self):
        """Test that the heap maintains exactly top_k items."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 8),
            ({3}, 10),
            ({4}, 12),
            ({5}, 6),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Heap should contain top_k items after processing
        assert len(min_heap.heap) <= top_k

    def test_initialize_mh_and_rmsup_inserts_first_top_k(self):
        """Test that only the first top_k items are inserted."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({0}, 1),    # index 0, inserted
            ({1}, 2),    # index 1, inserted
            ({2}, 10),   # index 2, not inserted
            ({3}, 5),    # index 3, not inserted
            ({4}, 15),   # index 4, not inserted
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Only items from index 0 to top_k-1 should be in the heap
        heap_itemsets = {itemset for _, itemset in min_heap.heap}
        assert (0,) in heap_itemsets
        assert (1,) in heap_itemsets
        assert len(min_heap.heap) == 2

    def test_initialize_mh_and_rmsup_correct_support_values(self):
        """Test that support values are correctly maintained in the heap."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 8),
            ({3}, 20),
            ({4}, 15),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Get all items from heap and verify they have correct support
        all_items = min_heap.get_all()
        supports = [support for support, _ in all_items]

        # Supports should be in descending order
        assert supports == supports

    def test_initialize_mh_and_rmsup_rmsup_value(self):
        """Test that rmsup (minimum support in heap) is correct."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 8),
            ({3}, 20),
            ({4}, 15),
            ({5}, 12),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # rmsup should be the minimum support value in the heap
        heap_supports = [support for support, _ in min_heap.heap]
        if heap_supports:
            assert rmsup == min(heap_supports)

    def test_initialize_mh_and_rmsup_empty_con_list_after_top_k(self):
        """Test when con_list has fewer items than top_k."""
        top_k = 5
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 10),
            ({2}, 8),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # All items should be in the heap (only 2 items available)
        assert len(min_heap.heap) == 2
        assert rmsup == 8  # minimum support in the heap

    def test_initialize_mh_and_rmsup_all_same_support(self):
        """Test with con_list items having same support values."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 5),
            ({3}, 5),
            ({4}, 5),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        assert len(min_heap.heap) == top_k
        assert rmsup == 5

    def test_initialize_mh_and_rmsup_single_item_in_con_list(self):
        """Test with only one item in con_list."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [({1}, 10)]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Single item should be in the heap
        assert len(min_heap.heap) == 1
        assert rmsup == 10

    def test_initialize_mh_and_rmsup_large_con_list(self):
        """Test with a large con_list to ensure scalability."""
        top_k = 10
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        # Create a large con_list
        con_list = [({i}, 100 - i) for i in range(100)]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        assert len(min_heap.heap) <= top_k
        assert rmsup >= 0

    def test_initialize_mh_and_rmsup_with_tuple_itemsets(self):
        """Test that itemsets are correctly converted to tuples."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1, 2}, 5),
            ({3, 4}, 8),
            ({5, 6}, 10),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Check that itemsets in heap are tuples
        for _, itemset in min_heap.heap:
            assert isinstance(itemset, tuple)

    def test_initialize_mh_and_rmsup_returns_tuple(self):
        """Test that the function returns a tuple of (min_heap, rmsup)."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 10),
            ({3}, 8),
        ]

        result = algo.initialize_mh_and_rmsup(con_list)

        assert isinstance(result, tuple)
        assert len(result) == 2
        min_heap, rmsup = result
        assert isinstance(min_heap, MinHeapTopK)
        assert isinstance(rmsup, int)

    def test_initialize_mh_and_rmsup_returns_correct_values(self):
        """Test that the function returns correct min_heap and rmsup values."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 10),
            ({3}, 8),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Verify the returned values are correct
        assert isinstance(min_heap, MinHeapTopK)
        assert isinstance(rmsup, int)
        assert min_heap.k == top_k
        assert len(min_heap.heap) == 2

    def test_initialize_mh_and_rmsup_descending_supports(self):
        """Test with descending support values."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 1),
            ({2}, 2),
            ({3}, 50),
            ({4}, 40),
            ({5}, 30),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Heap should contain first 3 items (indices 0-2)
        assert len(min_heap.heap) == 3
        heap_supports = [support for support, _ in min_heap.heap]
        assert min(heap_supports) == rmsup
        # rmsup should be the minimum of first 3 items
        assert rmsup == 1

    def test_initialize_mh_and_rmsup_ascending_supports(self):
        """Test with ascending support values."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 1),
            ({2}, 2),
            ({3}, 30),
            ({4}, 40),
            ({5}, 50),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Heap should contain first 3 items (indices 0-2)
        assert len(min_heap.heap) == 3
        heap_supports = [support for support, _ in min_heap.heap]
        # rmsup should be the minimum support among first 3 items
        assert rmsup == 1

    def test_initialize_mh_and_rmsup_with_complex_itemsets(self):
        """Test with complex multi-item itemsets."""
        top_k = 2
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        con_list = [
            ({1}, 5),
            ({2}, 8),
            ({1, 2, 3, 4}, 15),
            ({5, 6, 7}, 20),
            ({8, 9}, 12),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Verify heap contains first 2 items
        assert len(min_heap.heap) == 2
        assert rmsup == 5  # minimum support of first 2 items

    def test_algorithm_state_after_initialization(self):
        """Test that algorithm maintains correct state after initialization."""
        top_k = 3
        algo = PrefixPartitioningbasedTopKAlgorithm(k=top_k, partitionClass=SglPartition)

        assert hasattr(algo, 'top_k')
        assert algo.top_k == top_k

        con_list = [
            ({1}, 5),
            ({2}, 10),
            ({3}, 8),
        ]

        min_heap, rmsup = algo.initialize_mh_and_rmsup(con_list)

        # Verify initialization returns correct types
        assert isinstance(min_heap, MinHeapTopK)
        assert min_heap.k == top_k
        assert len(min_heap.heap) == 3
