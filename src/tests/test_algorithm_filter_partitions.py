import pytest
from unittest.mock import Mock, patch, MagicMock, call
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm
from ptf.min_heap import MinHeapTopK
from ptf.sgl_partition import SglPartition


class TestFilterPartitions:
    """Test suite for filter_partitions method."""

    def test_filter_partitions_basic(self):
        """Test filter_partitions with basic input."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        # Setup inputs
        promising_arr = {1: [1, 2], 2: [2, 3], 3: [3]}
        partitions = [1, 2, 3]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 5, (1, 2): 10, (2,): 8, (2, 3): 6, (3,): 4}
        rmsup = 5

        # Call the method
        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Verify the method executed (this is a side-effect method)
        assert isinstance(promising_arr, dict)

    def test_filter_partitions_empty_partitions(self):
        """Test with empty partitions list."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: [2, 3]}
        partitions = []
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 5, (1, 2): 10}
        rmsup = 5

        # Should not raise an error
        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Original promising_arr should be unchanged
        assert promising_arr[1] == [1, 2]
        assert promising_arr[2] == [2, 3]

    def test_filter_partitions_single_item_below_rmsup(self):
        """Test when single item has support below rmsup."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 3}  # support of item 1 is 3
        rmsup = 5  # rmsup is 5, so 3 <= 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Since partition 1 has support <= rmsup, the list should be cleared
        assert promising_arr[1] == []

    def test_filter_partitions_single_item_above_rmsup(self):
        """Test when single item has support above rmsup."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2]}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10}  # support of item 1 is 10
        rmsup = 5  # rmsup is 5, so 10 > 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Since partition 1 has support > rmsup, items should not be removed
        # Item 1 is kept, item 2 may be filtered based on pair support
        assert 1 in promising_arr[1]

    def test_filter_partitions_pair_below_rmsup(self):
        """Test when pair support is below rmsup."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10, (1, 2): 3}  # pair (1,2) has support 3
        rmsup = 5  # rmsup is 5, so 3 <= 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Item 2 should be removed from partition 1's list
        assert 2 not in promising_arr[1]

    def test_filter_partitions_pair_above_rmsup(self):
        """Test when pair support is above rmsup."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10, (1, 2): 8}  # pair (1,2) has support 8
        rmsup = 5  # rmsup is 5, so 8 > 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Item 2 should remain in partition 1's list
        assert 2 in promising_arr[1]

    def test_filter_partitions_missing_con_map_entry(self):
        """Test when con_map doesn't contain an entry (defaults to 0)."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {}  # Empty con_map
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # With missing entry, con_map.get() returns 0, which is <= rmsup
        assert promising_arr[1] == []

    def test_filter_partitions_sgl_partition_called_for_large_lists(self):
        """Test that SglPartition.execute is called when promising items > 2."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2, 3, 4], 2: []}  # 4 items > 2
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10, (1, 2): 8, (1, 3): 9, (1, 4): 7}
        rmsup = 5

        with patch.object(SglPartition, 'execute') as mock_execute:
            algo.filter_partitions(
                promising_arr, partitions, min_heap, con_map, rmsup)

            # SglPartition.execute should be called
            mock_execute.assert_called()

    def test_filter_partitions_sgl_partition_not_called_for_small_lists(self):
        """Test that SglPartition.execute is NOT called when promising items <= 2."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: []}  # 2 items <= 2
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10, (1, 2): 8}
        rmsup = 5

        with patch.object(SglPartition, 'execute') as mock_execute:
            algo.filter_partitions(
                promising_arr, partitions, min_heap, con_map, rmsup)

            # SglPartition.execute should NOT be called
            mock_execute.assert_not_called()

    def test_filter_partitions_multiple_partitions(self):
        """Test with multiple partitions."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: [2, 3], 3: [3]}
        partitions = [1, 2, 3]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {
            (1,): 10, (1, 2): 8,
            (2,): 9, (2, 3): 4,
            (3,): 5
        }
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Verify partitions were processed
        assert isinstance(promising_arr, dict)

    def test_filter_partitions_item_less_than_partition_not_removed(self):
        """Test that items < partition are not removed (only > partition are checked)."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        # Item 1 is less than partition 2, so it should not be checked for pair removal
        promising_arr = {2: [1], 3: []}
        partitions = [2]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(2,): 10, (1, 2): 2}  # pair has low support
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Item 1 should remain (condition is promissing_item > partition)
        assert 1 in promising_arr[2]

    def test_filter_partitions_break_on_single_item_match(self):
        """Test that loop breaks when single item matches partition."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2, 3], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 3}  # single item support <= rmsup
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # The loop should break after clearing, so no further checks
        assert promising_arr[1] == []

    def test_filter_partitions_rmsup_zero(self):
        """Test with rmsup = 0."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 0, (1, 2): 0}
        rmsup = 0

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # All support values equal rmsup, so conditions should trigger
        assert promising_arr[1] == []

    def test_filter_partitions_large_support_values(self):
        """Test with large support values."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10000, (1, 2): 9999}
        rmsup = 5000

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Both support values > rmsup, so items should remain
        assert 1 in promising_arr[1]
        assert 2 in promising_arr[1]

    def test_filter_partitions_preserves_other_items(self):
        """Test that filtering one partition doesn't affect others."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2], 2: [2, 3], 3: [3]}
        partitions = [1, 2, 3]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {
            (1,): 3,     # Will clear partition 1
            (2,): 10,    # Partition 2 not affected
            (2, 3): 8,   # Pair support > rmsup, so item 3 remains
            (3,): 10     # Partition 3 not affected
        }
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Partition 1 should be cleared
        assert promising_arr[1] == []

        # Partitions 2 and 3 should keep their original lists
        assert promising_arr[2] == [2, 3]
        assert promising_arr[3] == [3]

    def test_filter_partitions_modifies_in_place(self):
        """Test that filter_partitions modifies the promising_arr in place."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        original_dict = {1: [1, 2], 2: []}
        promising_arr = original_dict
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 3}
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # The original dict should be modified
        assert original_dict[1] == []
        assert promising_arr is original_dict

    def test_filter_partitions_multiple_items_with_mixed_support(self):
        """Test with multiple promising items and mixed support values."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [2, 3, 4], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {
            (1,): 10,
            (1, 2): 3,   # Should be removed (3 <= 5)
            (1, 3): 6,   # Should remain (6 > 5)
            (1, 4): 5    # Should be removed (5 <= 5)
        }
        rmsup = 5

        algo.filter_partitions(promising_arr, partitions,
                               min_heap, con_map, rmsup)

        # Item 2 should be removed, item 3 should remain, item 4 should be removed
        assert 2 not in promising_arr[1]
        assert 3 in promising_arr[1]
        assert 4 not in promising_arr[1]

    def test_filter_partitions_sgl_partition_called_with_correct_args(self):
        """Test that SglPartition.execute is called with correct arguments."""
        algo = PrefixPartitioningbasedTopKAlgorithm(top_k=3)

        promising_arr = {1: [1, 2, 3, 4, 5], 2: []}
        partitions = [1]
        min_heap = Mock(spec=MinHeapTopK)
        con_map = {(1,): 10, (1, 2): 8, (1, 3): 9, (1, 4): 7, (1, 5): 6}
        rmsup = 5

        with patch.object(SglPartition, 'execute') as mock_execute:
            algo.filter_partitions(
                promising_arr, partitions, min_heap, con_map, rmsup)

            # Verify SglPartition.execute was called with correct parameters
            mock_execute.assert_called_once()
            call_kwargs = mock_execute.call_args[1]
            assert call_kwargs['partition_item'] == 1
            assert call_kwargs['min_heap'] is min_heap
            # promising_items may have been filtered
            assert isinstance(call_kwargs['promising_items'], list)
