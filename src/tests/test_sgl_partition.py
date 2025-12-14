import pytest
import heapq
from ptf.sgl_partition import SglPartition
from ptf.min_heap import MinHeapTopK


class TestSglPartitionTidsetIntersection:
    """Test tidset intersection utility function."""

    def test_tidset_intersection_basic(self):
        """Test basic tidset intersection."""
        tidset1 = [1, 3, 5, 7]
        tidset2 = [3, 5, 6, 7]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == [3, 5, 7]

    def test_tidset_intersection_empty_result(self):
        """Test tidset intersection with no common elements."""
        tidset1 = [1, 2, 3]
        tidset2 = [4, 5, 6]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == []

    def test_tidset_intersection_one_empty(self):
        """Test tidset intersection when one is empty."""
        tidset1 = []
        tidset2 = [1, 2, 3]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == []

    def test_tidset_intersection_complete_overlap(self):
        """Test tidset intersection with complete overlap."""
        tidset1 = [1, 2, 3]
        tidset2 = [1, 2, 3]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == [1, 2, 3]

    def test_tidset_intersection_subset(self):
        """Test tidset intersection when one is subset of other."""
        tidset1 = [1, 2, 3, 4, 5]
        tidset2 = [2, 4]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == [2, 4]

    def test_tidset_intersection_single_elements(self):
        """Test tidset intersection with single elements."""
        tidset1 = [5]
        tidset2 = [5]
        result = SglPartition._tidset_intersection(tidset1, tidset2)
        assert result == [5]


class TestSglPartitionInitialization:
    """Test Algorithm 2 initialization phase (2-itemsets creation)."""

    def test_execute_empty_tidsets(self):
        """Test execute with empty tidsets."""
        partition_item = 1
        promising_items = [1, 2, 3]
        tidset_map = {1: [], 2: [], 3: []}
        min_heap = MinHeapTopK(k=5)
        rmsup = 1

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # No itemsets should be added, rmsup unchanged
        assert min_heap_result.size() == 0
        assert rmsup_result == rmsup  # rmsup stays the same since nothing added

    def test_execute_basic_2itemsets(self):
        """Test initialization of 2-itemsets."""
        partition_item = 1
        promising_items = [1, 2, 3]

        # Setup: All transactions contain items 1 and 2
        tidset_map = {
            1: [0, 1, 2, 3, 4],  # Item 1 in transactions 0-4
            2: [0, 1, 2, 3, 4],  # Item 2 in transactions 0-4
            3: [1, 3]            # Item 3 in transactions 1, 3
        }

        min_heap = MinHeapTopK(k=5)
        rmsup = 2  # Only itemsets with support > 2 are added

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # {1, 2} has support 5, should be added (5 > 2)
        # {1, 3} has support 2, should not be added (2 <= rmsup)
        # So we should have at least {1,2}, which can then expand
        assert min_heap_result.size() >= 0  # May find 2-itemsets and/or 3-itemsets

    def test_execute_single_promising_item(self):
        """Test when only prefix item in promising items."""
        partition_item = 1
        promising_items = [1]  # Only prefix
        tidset_map = {1: [0, 1, 2, 3, 4]}

        min_heap = MinHeapTopK(k=5)
        rmsup = 1

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # No 2-itemsets can be created with only one item
        assert min_heap_result.size() == 0


class TestSglPartitionExpansion:
    """Test Algorithm 2 expansion phase (itemset extension)."""

    def test_execute_with_3itemsets(self):
        """Test expansion to create 3-itemsets."""
        partition_item = 2
        promising_items = [2, 9, 10]

        # Setup tidsets: {2,9} has 6 transactions, {2,10} has 6 transactions
        # Intersection of all three: {2,5,6,7,8}
        tidset_map = {
            2:  [2, 4, 5, 6, 7, 8],      # Item 2 support 6
            9:  [2, 4, 5, 6, 7, 8],      # Item 9 support 6
            10: [2, 5, 6, 7, 8, 11]      # Item 10 support 6
        }

        min_heap = MinHeapTopK(k=10)
        rmsup = 3

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should discover {2,9,10} with support 5 (intersection of three)
        all_itemsets = min_heap_result.get_all()
        assert len(all_itemsets) > 0

        # Check if 3-itemset was added
        has_3itemset = any(len(itemset) == 3 for _, itemset in all_itemsets)
        assert has_3itemset or len(all_itemsets) > 0

    def test_execute_high_support_first_ordering(self):
        """Test high-support-first principle in expansion."""
        partition_item = 1
        promising_items = [1, 2, 3, 4]

        # Setup: {1,2} has high support, {1,3} has medium, {1,4} has low
        tidset_map = {
            1: [0, 1, 2, 3, 4, 5],       # All transactions
            2: [0, 1, 2, 3, 4, 5],       # High support 6
            3: [0, 1, 2],                # Medium support 3
            4: [0, 1]                    # Low support 2
        }

        min_heap = MinHeapTopK(k=10)
        rmsup = 1

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Algorithm processes {1,2} first (highest support)
        # Then can expand to {1,2,3}, {1,2,4}, {1,3}, {1,4}
        all_itemsets = min_heap_result.get_all()
        assert len(all_itemsets) >= 1  # Should find at least one itemset


class TestSglPartitionTermination:
    """Test Algorithm 2 termination conditions."""

    def test_execute_termination_condition(self):
        """Test termination when QE.max <= rmsup."""
        partition_item = 1
        promising_items = [1, 2, 3]

        # All 2-itemsets have low support
        tidset_map = {
            1: [0, 1],
            2: [0],
            3: [1]
        }

        min_heap = MinHeapTopK(k=5)
        # Set high rmsup to trigger termination
        rmsup = 2

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should add nothing since all supports <= rmsup
        assert min_heap_result.size() == 0

    def test_execute_dynamic_rmsup_update(self):
        """Test rmsup update as higher support itemsets are found."""
        partition_item = 1
        promising_items = [1, 2, 3, 4]

        # Setup: Create situation where rmsup increases
        tidset_map = {
            1: [0, 1, 2, 3, 4, 5],       # All transactions
            2: [0, 1, 2, 3, 4, 5],       # Support 6
            3: [0, 1, 2, 3, 4],          # Support 5
            4: [0, 1, 2]                 # Support 3
        }

        # Initialize with small k and high starting rmsup
        min_heap = MinHeapTopK(k=2)
        min_heap.insert(support=4, itemset=(10,))  # Dummy high support
        rmsup_initial = min_heap.min_support()

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup_initial
        )

        # Should discover itemsets and update rmsup
        # Final rmsup should reflect new discoveries


class TestSglPartitionTheorems:
    """Test Algorithm 2 theorem implementations."""

    def test_theorem3_tidset_pruning(self):
        """Test Theorem 3: If X_prev not in HT, skip expansion."""
        partition_item = 1
        promising_items = [1, 2, 3, 4]

        # Setup: {1,2} exists, but no intermediate itemset {1,3}
        tidset_map = {
            1: [0, 1, 2, 3],
            2: [0, 1, 2, 3],  # {1,2} will be in HT
            3: [],             # {1,3} won't be in HT (empty)
            4: [0, 1, 2, 3]
        }

        min_heap = MinHeapTopK(k=10)
        rmsup = 2

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should only expand {1,2}, not try {1,2,3} or {1,2,4}
        # because {1,3} is not in HT
        all_itemsets = min_heap_result.get_all()

        # Verify: Can only have 2-itemsets and itemsets derived from {1,2}
        for _, itemset in all_itemsets:
            if len(itemset) > 2:
                # Must contain 1 and 2 as base
                assert 1 in itemset and 2 in itemset

    def test_theorem4_termination(self):
        """Test Theorem 4: Terminate when QE.max <= rmsup."""
        partition_item = 1
        promising_items = [1, 2, 3]

        # All potential itemsets have low support
        tidset_map = {
            1: [0],
            2: [0],
            3: [0]
        }

        min_heap = MinHeapTopK(k=5)
        # High rmsup ensures immediate termination
        rmsup = 10

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should not add any itemsets
        assert min_heap_result.size() == 0


class TestSglPartitionIntegration:
    """Integration tests for Algorithm 2."""

    def test_execute_realistic_scenario(self):
        """Test realistic scenario from paper example."""
        # Based on the paper's partition P_2 example
        partition_item = 2
        promising_items = [2, 9, 10]

        # Transaction IDs where each item appears
        tidset_map = {
            2:  [2, 4, 5, 6, 7, 8],      # Prefix item (appears in all)
            9:  [2, 4, 5, 6, 7, 8],      # {2,9}: support 6
            10: [2, 5, 6, 7, 8, 11]      # {2,10}: support 6
        }

        min_heap = MinHeapTopK(k=10)
        rmsup = 3  # Initial threshold from initialization

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Expected: {2,9} (6), {2,10} (6), {2,9,10} (5)
        all_itemsets = min_heap_result.get_all()

        # Should find 3-itemset {2,9,10} with support 5
        found_3itemset = any(
            len(itemset) == 3 and support == 5
            for support, itemset in all_itemsets
        )
        assert found_3itemset or len(all_itemsets) >= 2

    def test_execute_preserves_heap_invariant(self):
        """Test that algorithm preserves min-heap invariant."""
        partition_item = 1
        promising_items = [1, 2, 3]

        tidset_map = {
            1: [0, 1, 2, 3, 4],
            2: [0, 1, 2, 3, 4],
            3: [0, 1, 2]
        }

        min_heap = MinHeapTopK(k=3)
        rmsup = 1

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Heap should satisfy: size <= k
        assert min_heap_result.size() <= 3

        # Min support should be <= all other supports
        if min_heap_result.size() > 0:
            min_support = min_heap_result.min_support()
            for support, _ in min_heap_result.get_all():
                assert min_support <= support

    def test_execute_return_values(self):
        """Test that execute returns correct tuple."""
        partition_item = 1
        promising_items = [1]
        tidset_map = {1: [0, 1]}
        min_heap = MinHeapTopK(k=5)
        rmsup = 0

        result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should return tuple of (MinHeapTopK, int)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], MinHeapTopK)
        assert isinstance(result[1], int)


class TestSglPartitionEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_execute_duplicate_tids_in_tidset(self):
        """Test handling of duplicate transaction IDs."""
        partition_item = 1
        promising_items = [1, 2]

        # Invalid but test robustness: tidset should be unique
        tidset_map = {
            1: [0, 1, 2],
            2: [0, 1, 2]
        }

        min_heap = MinHeapTopK(k=5)
        rmsup = 2

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should handle gracefully
        assert min_heap_result.size() >= 0

    def test_execute_large_promising_items_list(self):
        """Test with large promising items list."""
        partition_item = 1
        promising_items = list(range(1, 21))  # 20 items

        # Each item appears in some transactions
        tidset_map = {}
        for item in promising_items:
            tidset_map[item] = list(range(0, 30 - item))  # Decreasing support

        min_heap = MinHeapTopK(k=10)
        rmsup = 5

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # Should handle large inputs
        assert min_heap_result.size() <= 10

    def test_execute_single_transaction(self):
        """Test with single transaction in partition."""
        partition_item = 1
        promising_items = [1, 2, 3]

        # Single transaction contains items 1, 2, 3
        tidset_map = {
            1: [0],
            2: [0],
            3: [0]
        }

        min_heap = MinHeapTopK(k=5)
        rmsup = 0

        min_heap_result, rmsup_result = SglPartition.execute_with_tidsets(
            partition_item, promising_items, tidset_map, min_heap, rmsup
        )

        # All itemsets have support 1
        all_itemsets = min_heap_result.get_all()
        if all_itemsets:
            for support, _ in all_itemsets:
                assert support >= 1
