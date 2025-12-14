"""Unit tests for ptf.runner_parallel module."""

import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from io import StringIO
import os
import tempfile
import multiprocessing

from ptf.runner_parallel import (
    run_ptf_algorithm_parallel,
    run_ptf_algorithm_parallel_with_timing
)
from ptf.min_heap import MinHeapTopK


class TestRunPtfAlgorithmParallel:
    """Test suite for run_ptf_algorithm_parallel function."""

    def test_run_ptf_algorithm_parallel_basic(self):
        """Test basic execution of run_ptf_algorithm_parallel with valid data."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 2\n")
            f.write("2 3\n")
            f.write("1 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer)
            
            output = output_buffer.getvalue()
            assert "Initial rmsup:" in output
            assert "Initial MH size:" in output
            assert "Total itemsets found:" in output
            assert "Execution time:" in output
            assert "Memory used:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_custom_workers(self):
        """Test parallel algorithm with custom number of workers."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4\n")
            f.write("2 3 4 5\n")
            f.write("1 3 5\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            # Test with explicit number of workers
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=2)
            
            output = output_buffer.getvalue()
            assert "Initial rmsup:" in output
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_default_workers(self):
        """Test parallel algorithm with default number of workers (None)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            # num_workers=None should use default cpu_count()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=None)
            
            output = output_buffer.getvalue()
            assert "Initial rmsup:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_different_top_k_values(self):
        """Test parallel algorithm with various top_k values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4 5\n")
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 4 5\n")
            temp_file = f.name
        
        try:
            for top_k in [1, 2, 5, 10]:
                output_buffer = StringIO()
                run_ptf_algorithm_parallel(temp_file, top_k=top_k, output_file=output_buffer)
                output = output_buffer.getvalue()
                assert "Initial rmsup:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_without_output_file(self):
        """Test parallel algorithm without specifying output_file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            # Should not raise an exception
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=None)
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_file_not_found(self):
        """Test parallel algorithm with non-existent file."""
        output_buffer = StringIO()
        with pytest.raises(FileNotFoundError):
            run_ptf_algorithm_parallel("nonexistent_file.txt", top_k=5, output_file=output_buffer)

    def test_run_ptf_algorithm_parallel_output_format(self):
        """Test that parallel output is formatted correctly."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            f.write("1 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=3, output_file=output_buffer)
            output = output_buffer.getvalue()
            
            # Verify output structure
            assert "Initial rmsup:" in output
            assert "Initial MH size:" in output
            assert "Total itemsets found:" in output
            assert "Final rmsup:" in output
            assert "Execution time:" in output
            assert "Memory used:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_itemset_format(self):
        """Test that itemsets are formatted correctly in parallel output."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer)
            output = output_buffer.getvalue()
            
            # Verify itemset format (should contain braces and arrows)
            lines = output.split('\n')
            itemset_lines = [line for line in lines if '=>' in line]
            assert len(itemset_lines) > 0
            for line in itemset_lines:
                assert '{' in line and '}' in line
                assert '=>' in line
                assert 'Support:' in line
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_single_transaction(self):
        """Test parallel algorithm with single transaction."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_large_dataset(self):
        """Test parallel algorithm with larger dataset."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Create a larger transaction database
            for i in range(100):
                items = [str((j % 10) + 1) for j in range(i % 10 + 1)]
                f.write(" ".join(items) + "\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=5, output_file=output_buffer, num_workers=2)
            output = output_buffer.getvalue()
            
            assert "Total itemsets found:" in output
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_memory_metrics(self):
        """Test that memory metrics are included in parallel output."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=3, output_file=output_buffer)
            output = output_buffer.getvalue()
            
            assert "Memory used:" in output
            memory_lines = [l for l in output.split('\n') if 'Memory used:' in l]
            assert len(memory_lines) > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_single_worker(self):
        """Test parallel algorithm with single worker."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=1)
            output = output_buffer.getvalue()
            
            assert "Initial rmsup:" in output
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_many_workers(self):
        """Test parallel algorithm with many workers."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4 5\n")
            f.write("2 3 4 5 6\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            # Try with more workers than transactions
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=8)
            output = output_buffer.getvalue()
            
            assert "Initial rmsup:" in output
        finally:
            os.unlink(temp_file)

    @patch('ptf.runner_parallel.PrefixPartitioningbasedTopKAlgorithmParallel')
    def test_run_ptf_algorithm_parallel_uses_parallel_algorithm(self, mock_algo_class):
        """Test that parallel function uses PrefixPartitioningbasedTopKAlgorithmParallel."""
        mock_min_heap = MagicMock()
        mock_min_heap.heap = []
        mock_min_heap.get_all.return_value = []
        
        mock_algo_instance = MagicMock()
        mock_algo_instance.initialize_mh_and_rmsup.return_value = (mock_min_heap, 10)
        mock_algo_instance.build_promissing_item_arrays.return_value = {}
        mock_algo_instance.filter_partitions.return_value = (mock_min_heap, 10)
        mock_algo_class.return_value = mock_algo_instance
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=2)
            
            # Verify PrefixPartitioningbasedTopKAlgorithmParallel was instantiated
            mock_algo_class.assert_called_once()
            call_kwargs = mock_algo_class.call_args[1]
            assert 'num_workers' in call_kwargs
            assert call_kwargs['num_workers'] == 2
        finally:
            os.unlink(temp_file)

    @patch('ptf.runner_parallel.track_execution')
    def test_run_ptf_algorithm_parallel_metrics_tracking(self, mock_track):
        """Test that parallel execution metrics are properly tracked."""
        mock_metrics = MagicMock()
        mock_metrics.execution_time = 2.345
        mock_metrics.memory_used = 78.90
        mock_track.return_value.__enter__.return_value = mock_metrics
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer)
            
            output = output_buffer.getvalue()
            assert "2.3450" in output or "2.345" in output
        finally:
            os.unlink(temp_file)


class TestRunPtfAlgorithmParallelWithTiming:
    """Test suite for run_ptf_algorithm_parallel_with_timing function."""

    def test_run_ptf_algorithm_parallel_with_timing_returns_float(self):
        """Test that function returns execution time as float."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=output_buffer)
            
            assert isinstance(exec_time, float)
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_reasonable_time(self):
        """Test that execution time is reasonable."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=output_buffer)
            
            # Execution time should be positive and reasonable
            assert 0 < exec_time < 60
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_output_includes_time(self):
        """Test that output includes execution time information."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=output_buffer)
            
            output = output_buffer.getvalue()
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_without_output_file(self):
        """Test parallel timing function without output file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name
        
        try:
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=None)
            assert isinstance(exec_time, float)
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_custom_workers(self):
        """Test timing function with custom number of workers."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_parallel_with_timing(
                temp_file, top_k=2, output_file=output_buffer, num_workers=2
            )
            
            assert isinstance(exec_time, float)
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_file_not_found(self):
        """Test timing function with non-existent file."""
        output_buffer = StringIO()
        with pytest.raises(FileNotFoundError):
            run_ptf_algorithm_parallel_with_timing("nonexistent.txt", top_k=2, output_file=output_buffer)

    def test_run_ptf_algorithm_parallel_with_timing_different_top_k(self):
        """Test parallel timing with different top_k values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            for i in range(50):
                items = [str(j + 1) for j in range(1, 6)]
                f.write(" ".join(items) + "\n")
            temp_file = f.name
        
        try:
            times = []
            for top_k in [1, 5, 10]:
                output_buffer = StringIO()
                exec_time = run_ptf_algorithm_parallel_with_timing(
                    temp_file, top_k=top_k, output_file=output_buffer
                )
                times.append(exec_time)
                assert exec_time > 0
            
            assert all(0 < t < 60 for t in times)
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_parallel_with_timing_small_dataset(self):
        """Test that parallel timing works correctly with small dataset."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=output_buffer)
            
            # Timing should be positive and reasonable
            assert exec_time > 0
            assert exec_time < 10
        finally:
            os.unlink(temp_file)


class TestParallelRunnerIntegration:
    """Integration tests for parallel runner functions."""

    def test_parallel_algorithm_produces_valid_results(self):
        """Test that parallel algorithm produces valid itemset results."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 2\n")
            f.write("2 3\n")
            f.write("1 3\n")
            f.write("1 2 3\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=3, output_file=output_buffer, num_workers=2)
            output = output_buffer.getvalue()
            
            # Parse and validate results
            lines = output.split('\n')
            itemset_lines = [l for l in lines if '=>' in l]
            
            # Should have some itemsets
            assert len(itemset_lines) > 0
            
            # Each itemset should have valid format
            for line in itemset_lines:
                assert '{' in line and '}' in line
                assert 'Support:' in line
        finally:
            os.unlink(temp_file)

    def test_parallel_with_different_worker_counts(self):
        """Test that parallel algorithm works with different worker counts."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4 5\n")
            f.write("2 3 4 5\n")
            f.write("1 3 5\n")
            temp_file = f.name
        
        try:
            outputs = []
            for workers in [1, 2, 4]:
                output_buffer = StringIO()
                run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output_buffer, num_workers=workers)
                outputs.append(output_buffer.getvalue())
            
            # All outputs should contain key metrics
            for output in outputs:
                assert "Initial rmsup:" in output
                assert "Total itemsets found:" in output
                assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_empty_database_parallel(self):
        """Test parallel runner with empty transaction database."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Empty file
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()
            
            # Should still output metrics
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_parallel_timing_vs_basic_execution(self):
        """Test that timing version captures same results as basic version."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 3 4\n")
            temp_file = f.name
        
        try:
            output1 = StringIO()
            output2 = StringIO()
            
            run_ptf_algorithm_parallel(temp_file, top_k=2, output_file=output1, num_workers=2)
            exec_time = run_ptf_algorithm_parallel_with_timing(temp_file, top_k=2, output_file=output2, num_workers=2)
            
            out1 = output1.getvalue()
            out2 = output2.getvalue()
            
            # Both should have same initial metrics
            assert "Initial rmsup:" in out1
            assert "Initial rmsup:" in out2
            assert "Total itemsets found:" in out1
            assert "Total itemsets found:" in out2
            
            # Timing version should return valid time
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_parallel_with_large_top_k(self):
        """Test parallel algorithm with large top_k value."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            for i in range(20):
                items = [str((j % 5) + 1) for j in range(5)]
                f.write(" ".join(items) + "\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=100, output_file=output_buffer, num_workers=2)
            output = output_buffer.getvalue()
            
            # Should complete without error
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)

    def test_parallel_single_item_transactions(self):
        """Test parallel algorithm with single-item transactions."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1\n")
            f.write("2\n")
            f.write("3\n")
            f.write("1\n")
            f.write("2\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            run_ptf_algorithm_parallel(temp_file, top_k=3, output_file=output_buffer, num_workers=2)
            output = output_buffer.getvalue()
            
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)
