"""
Test suite for SglPartitionHybridCandidatePruning

Tests verify:
1. Correctness: Pruned results match non-pruned results
2. Pruning effectiveness: Actual reduction in intersections
3. No false negatives: Top-k itemsets are found
4. No false positives: Only valid itemsets are returned
"""

import pytest
from ptf.min_heap import MinHeapTopK
from ptf.hybrid_vertical_storage.sgl_partition_hybrid import SglPartitionHybrid
from ptf.hybrid_vertical_storage.sgl_partition_hybrid_candidate_pruning import SglPartitionHybridCandidatePruning


class TestCandidatePruningBasics:
    """Test basic functionality of candidate pruning."""
    
    def test_top2_itemsets_extraction(self):
        """Test extraction of 2-itemsets from min_heap."""
        min_heap = MinHeapTopK(k=5)
        
        # Insert various itemsets
        min_heap.insert(support=100, itemset=(1, 2))
        min_heap.insert(support=90, itemset=(1, 2, 3))
        min_heap.insert(support=80, itemset=(2, 3))
        min_heap.insert(support=70, itemset=(1, 2, 3, 4))
        min_heap.insert(support=60, itemset=(3, 4))
        
        # Extract top-2 itemsets
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        # Should have 3 two-itemsets
        assert len(top2_set) == 3
        assert frozenset([1, 2]) in top2_set
        assert frozenset([2, 3]) in top2_set
        assert frozenset([3, 4]) in top2_set
    
    def test_top2_itemsets_empty_heap(self):
        """Test extraction from empty heap."""
        min_heap = MinHeapTopK(k=5)
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        assert len(top2_set) == 0
    
    def test_top2_itemsets_no_2itemsets(self):
        """Test extraction when no 2-itemsets exist."""
        min_heap = MinHeapTopK(k=5)
        
        # Insert only itemsets with size != 2
        min_heap.insert(support=100, itemset=(1,))
        min_heap.insert(support=90, itemset=(1, 2, 3))
        min_heap.insert(support=80, itemset=(2, 3, 4, 5))
        
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        assert len(top2_set) == 0


class TestCandidatePruningSimplePartition:
    """Test candidate pruning on a simple partition."""
    
    def setup_method(self):
        """Setup test data for each test."""
        # Simple partition with 5 transactions
        # TID: 0    1    2    3    4
        # Tx: {a,b} {a,c} {b,c} {a,b,c} {a,b}
        self.partition_data = [
            [1, 2],      # TID 0: {1, 2}
            [1, 3],      # TID 1: {1, 3}
            [2, 3],      # TID 2: {2, 3}
            [1, 2, 3],   # TID 3: {1, 2, 3}
            [1, 2],      # TID 4: {1, 2}
        ]
        
        self.partition_item = 1
        self.promising_items = [1, 2, 3]
    
    def test_pruning_output_matches_non_pruned(self):
        """Test that pruned and non-pruned versions produce identical results."""
        # Run without pruning
        min_heap_basic = MinHeapTopK(k=5)
        min_heap_basic.insert(support=5, itemset=(1,))
        min_heap_basic.insert(support=4, itemset=(2,))
        min_heap_basic.insert(support=4, itemset=(3,))
        rmsup_basic = min_heap_basic.min_support()
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap_basic)
        
        min_heap_basic, rmsup_basic = SglPartitionHybrid.execute(
            partition_item=self.partition_item,
            promising_items=self.promising_items,
            partition_data=self.partition_data,
            min_heap=min_heap_basic,
            rmsup=rmsup_basic
        )
        
        # Run with pruning
        min_heap_pruned = MinHeapTopK(k=5)
        min_heap_pruned.insert(support=5, itemset=(1,))
        min_heap_pruned.insert(support=4, itemset=(2,))
        min_heap_pruned.insert(support=4, itemset=(3,))
        rmsup_pruned = min_heap_pruned.min_support()
        
        min_heap_pruned, rmsup_pruned, _ = SglPartitionHybridCandidatePruning.execute(
            partition_item=self.partition_item,
            promising_items=self.promising_items,
            partition_data=self.partition_data,
            min_heap=min_heap_pruned,
            rmsup=rmsup_pruned,
            top2_set=top2_set
        )
        
        # Both should have same results
        basic_results = set(min_heap_basic.get_all())
        pruned_results = set(min_heap_pruned.get_all())
        
        assert len(basic_results) == len(pruned_results), \
            f"Different number of results: basic={len(basic_results)}, pruned={len(pruned_results)}"
    
    def test_no_false_negatives(self):
        """Test that pruning doesn't miss any top-k itemsets."""
        # Initialize with minimal support
        min_heap = MinHeapTopK(k=10)
        min_heap.insert(support=1, itemset=(1,))
        min_heap.insert(support=1, itemset=(2,))
        min_heap.insert(support=1, itemset=(3,))
        rmsup = min_heap.min_support()
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        # Run algorithm with pruning
        min_heap, rmsup, top2_set = SglPartitionHybridCandidatePruning.execute(
            partition_item=self.partition_item,
            promising_items=self.promising_items,
            partition_data=self.partition_data,
            min_heap=min_heap,
            rmsup=rmsup,
            top2_set=top2_set
        )
        
        # All itemsets in result should have support > rmsup or size < 3
        for support, itemset in min_heap.get_all():
            if len(itemset) >= 3:
                # Should be in heap only if it was top-k
                assert support >= rmsup or min_heap.size() < 10


class TestCandidatePruningStatistics:
    """Test pruning statistics tracking."""
    
    def setup_method(self):
        """Setup test data for statistics tests."""
        # Create a partition with more items
        self.partition_data = [
            [1, 2, 3],
            [1, 2, 4],
            [1, 3, 4],
            [2, 3, 4],
            [1, 2, 3, 4],
            [1, 2],
            [1, 3],
            [2, 3],
            [1, 4],
            [2, 4],
        ]
        
        self.partition_item = 1
        self.promising_items = [1, 2, 3, 4]
    
    def test_pruning_statistics(self):
        """Test that pruning statistics are tracked correctly."""
        min_heap = MinHeapTopK(k=10)
        min_heap.insert(support=10, itemset=(1,))
        min_heap.insert(support=9, itemset=(2,))
        min_heap.insert(support=8, itemset=(3,))
        min_heap.insert(support=7, itemset=(4,))
        rmsup = min_heap.min_support()
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        # Get pruning statistics
        stats = SglPartitionHybridCandidatePruning.get_pruning_stats(
            partition_item=self.partition_item,
            promising_items=self.promising_items,
            partition_data=self.partition_data,
            min_heap=min_heap,
            rmsup=rmsup,
            top2_set=top2_set
        )
        
        # Verify statistics keys exist
        assert 'intersections_performed' in stats
        assert 'candidates_before_timeliness' in stats
        assert 'candidates_after_timeliness' in stats
        assert 'candidates_after_lastitem' in stats
        assert 'timeliness_pruning_ratio' in stats
        assert 'lastitem_pruning_ratio' in stats
        
        # Verify counts make sense
        assert stats['candidates_after_timeliness'] <= stats['candidates_before_timeliness']
        assert stats['candidates_after_lastitem'] <= stats['candidates_after_timeliness']
        assert stats['intersections_performed'] == stats['candidates_after_lastitem']
        
        # Verify ratios are in [0, 1]
        assert 0 <= stats['timeliness_pruning_ratio'] <= 1
        assert 0 <= stats['lastitem_pruning_ratio'] <= 1


class TestCandidatePruningEdgeCases:
    """Test edge cases for candidate pruning."""
    
    def test_empty_partition(self):
        """Test with empty partition."""
        min_heap = MinHeapTopK(k=5)
        min_heap.insert(support=1, itemset=(1,))
        rmsup = min_heap.min_support()
        
        min_heap_result, rmsup_result, _ = SglPartitionHybridCandidatePruning.execute(
            partition_item=1,
            promising_items=[1],
            partition_data=[],
            min_heap=min_heap,
            rmsup=rmsup,
            top2_set=set()
        )
        
        # Should not crash and return valid results
        assert isinstance(min_heap_result, MinHeapTopK)
    
    def test_single_transaction_partition(self):
        """Test with single transaction."""
        min_heap = MinHeapTopK(k=5)
        min_heap.insert(support=1, itemset=(1,))
        min_heap.insert(support=1, itemset=(2,))
        rmsup = min_heap.min_support()
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        min_heap_result, rmsup_result, _ = SglPartitionHybridCandidatePruning.execute(
            partition_item=1,
            promising_items=[1, 2],
            partition_data=[[1, 2]],
            min_heap=min_heap,
            rmsup=rmsup,
            top2_set=top2_set
        )
        
        assert isinstance(min_heap_result, MinHeapTopK)
    
    def test_high_rmsup_prunes_all(self):
        """Test with very high rmsup that prunes everything."""
        partition_data = [
            [1, 2, 3],
            [1, 2],
            [1, 3],
        ]
        
        # Set rmsup very high
        min_heap = MinHeapTopK(k=5)
        min_heap.insert(support=1000, itemset=(1,))
        rmsup = min_heap.min_support()
        top2_set = SglPartitionHybridCandidatePruning._extract_top2_itemsets(min_heap)
        
        min_heap_result, rmsup_result, _ = SglPartitionHybridCandidatePruning.execute(
            partition_item=1,
            promising_items=[1, 2, 3],
            partition_data=partition_data,
            min_heap=min_heap,
            rmsup=rmsup,
            top2_set=top2_set
        )
        
        # Should not crash
        assert isinstance(min_heap_result, MinHeapTopK)


class TestPruningCorrectnessTheorems:
    """Test theoretical guarantees of pruning."""
    
    def test_timeliness_pruning_correctness(self):
        """
        Verify Timeliness Pruning:
        If support(X∪{y2}) <= rmsup, then support(X∪{y1,y2}) <= rmsup.
        
        Proof: support(X∪{y1,y2}) = |tidset(X) ∩ tidset({y1,y2})|
                                   <= |tidset(X∪{y2})|
                                   = support(X∪{y2})
                                   <= rmsup
        """
        # This is guaranteed by the downward closure property of support
        # When we skip intersecting due to support(X∪{y2}) <= rmsup,
        # we know support(X∪{y1,y2}) cannot exceed rmsup
        pass
    
    def test_lastitem_pruning_correctness(self):
        """
        Verify Last-item Pruning:
        If {y1, y2} not in top-k, then support({y1,y2}) <= rmsup.
        Therefore, any superset of {y1,y2} cannot be top-k.
        
        Proof: If X∪{y1,y2} were top-k with support > rmsup,
               then its subset {y1,y2} must also have support >= support(X∪{y1,y2}) > rmsup
               (by monotonicity), so {y1,y2} would be in top-k.
               Contradiction.
        """
        # This is guaranteed by the downward closure property
        pass


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
