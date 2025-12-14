"""
Integration tests for SglPartitionHybrid with hybrid vertical storage.

Verifies that the new hybrid version produces identical results to the old SglPartition.
"""

import pytest
from ptf.sgl_partition import SglPartition
from ptf.hybrid_vertical_storage.sgl_partition_hybrid import SglPartitionHybrid
from ptf.min_heap import MinHeapTopK


class TestSglPartitionHybridIntegration:
    """Test SglPartition integration with hybrid vertical storage."""
    
    def test_simple_partition_execution(self):
        """Test basic partition processing with hybrid storage."""
        partition_data = [
            [1, 2],
            [1, 3],
            [1, 2, 3],
            [1, 4],
        ]
        
        promising_items = [1, 2, 3, 4]
        min_heap = MinHeapTopK(k=10)
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=1
        )
        
        # Should find frequent itemsets
        assert result_heap is not None
        assert result_rmsup >= 1
    
    def test_backward_compatibility_with_tidsets(self):
        """Test backward compatibility with old tidset_map API."""
        # Create a simple tidset_map manually
        tidset_map = {
            1: [0, 1, 2, 3],  # Item 1 in all 4 transactions
            2: [0, 2],         # Item 2 in transactions 0, 2
            3: [1, 2],         # Item 3 in transactions 1, 2
            4: [3],            # Item 4 in transaction 3
        }
        
        promising_items = [1, 2, 3, 4]
        min_heap = MinHeapTopK(k=10)
        
        # Call with old API
        result_heap, result_rmsup = SglPartition.execute_with_tidsets(
            partition_item=1,
            promising_items=promising_items,
            tidset_map=tidset_map,
            min_heap=min_heap,
            rmsup=1
        )
        
        assert result_heap is not None
        assert result_rmsup >= 1
    
    def test_identical_results_old_vs_new(self):
        """
        Test that hybrid version produces same support values as old version.
        
        This is a critical test for correctness.
        """
        # Create test data
        partition_data = [
            [1, 2, 5],
            [1, 3, 5],
            [1, 2, 3],
            [1, 2, 4],
            [1, 3, 4],
        ]
        
        promising_items = [1, 2, 3, 4, 5]
        
        # Old API: build traditional tidset_map
        tidset_map = {}
        for item in promising_items:
            if item != 1:
                tidset_map[item] = []
            else:
                tidset_map[item] = list(range(len(partition_data)))
        
        for local_tid, transaction in enumerate(partition_data):
            for item in transaction:
                if item in tidset_map and item != 1:
                    tidset_map[item].append(local_tid)
        
        for item in tidset_map:
            tidset_map[item].sort()
        
        # Run old version
        min_heap_old = MinHeapTopK(k=10)
        result_old, rmsup_old = SglPartition.execute_with_tidsets(
            partition_item=1,
            promising_items=promising_items,
            tidset_map=tidset_map,
            min_heap=min_heap_old,
            rmsup=1
        )
        
        # Run new version
        min_heap_new = MinHeapTopK(k=10)
        result_new, rmsup_new = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap_new,
            rmsup=1
        )
        
        # Compare results
        # Both should have same rmsup (or very close)
        assert rmsup_old == rmsup_new, f"rmsup mismatch: {rmsup_old} vs {rmsup_new}"
        
        # Both should have same itemsets in heap
        old_items = set()
        new_items = set()
        
        # get_all() returns (support, itemset) tuples
        for support, itemset in result_old.get_all():
            old_items.add((itemset, support))
        
        for support, itemset in result_new.get_all():
            new_items.add((itemset, support))
        
        assert old_items == new_items, f"Itemset mismatch:\nOld: {old_items}\nNew: {new_items}"
    
    def test_empty_partition(self):
        """Test handling of empty partition."""
        partition_data = []
        promising_items = [1, 2, 3]
        min_heap = MinHeapTopK(k=10)
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=1
        )
        
        # Should handle gracefully
        assert result_heap is not None
    
    def test_single_transaction_partition(self):
        """Test partition with single transaction."""
        partition_data = [[1, 2, 3, 4, 5]]
        promising_items = [1, 2, 3, 4, 5]
        min_heap = MinHeapTopK(k=10)
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=1
        )
        
        # All items have support 1, should find 2-itemsets
        assert result_heap is not None
    
    def test_high_support_threshold(self):
        """Test with high support threshold that filters all itemsets."""
        partition_data = [
            [1, 2],
            [1, 3],
            [1, 4],
        ]
        
        promising_items = [1, 2, 3, 4]
        min_heap = MinHeapTopK(k=10)  # High threshold
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=100
        )
        
        # Should not find any frequent itemsets
        assert result_rmsup == 100
    
    def test_low_support_threshold(self):
        """Test with low support threshold."""
        partition_data = [
            [1, 2],
            [1, 3],
            [1, 2, 3],
            [1, 4],
            [1, 5],
        ]
        
        promising_items = [1, 2, 3, 4, 5]
        min_heap = MinHeapTopK(k=5)
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=1
        )
        
        # Should handle execution without error
        assert result_heap is not None
    
    def test_support_values_correctness(self):
        """
        Test that computed support values are correct for hybrid storage.
        """
        partition_data = [
            [1, 2],
            [1, 2],
            [1, 2],
            [1, 3],
        ]
        
        promising_items = [1, 2, 3]
        min_heap = MinHeapTopK(k=10)
        
        result_heap, _ = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=1
        )
        
        # Itemset {1,2} should have support 3
        # Itemset {1,3} should have support 1
        itemsets = result_heap.get_all()
        
        # get_all() returns (support, itemset) tuples
        for support, itemset in itemsets:
            if itemset == (1, 2):
                assert support == 3, f"Support for (1,2) should be 3, got {support}"
            elif itemset == (1, 3):
                assert support == 1, f"Support for (1,3) should be 1, got {support}"
    
    def test_many_items_different_formats(self):
        """
        Test with many items that will be stored in different formats.
        
        This exercises the format selection logic:
        - Some items with low support (tid-list)
        - Some items with high support (dif-list)
        - Some items with medium support (bit-vector)
        """
        # Create partition with 100 transactions
        partition_data = []
        
        # Item 2: appears in first 10 transactions (low support)
        # Item 3: appears in first 50 transactions (medium support)
        # Item 4: appears in first 90 transactions (high support)
        
        for i in range(100):
            transaction = [1]  # Prefix item
            if i < 10:
                transaction.append(2)  # Low support
            if i < 50:
                transaction.append(3)  # Medium support
            if i < 90:
                transaction.append(4)  # High support
            partition_data.append(transaction)
        
        promising_items = [1, 2, 3, 4]
        min_heap = MinHeapTopK(k=10)
        
        result_heap, result_rmsup = SglPartitionHybrid.execute(
            partition_item=1,
            promising_items=promising_items,
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=5
        )
        
        # Should handle all formats correctly
        assert result_heap is not None
        itemsets = result_heap.get_all()
        
        # Should find itemsets with sufficient support
        # get_all() returns (support, itemset) tuples sorted by support descending
        found_supports = [support for support, _ in itemsets]
        assert all(s >= 5 for s in found_supports), f"All supports should be >= 5, got {found_supports}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
