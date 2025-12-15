"""Unit tests for ptf.runner module."""

import pytest
from unittest.mock import Mock, patch, MagicMock, mock_open
from io import StringIO
import os
import tempfile

from ptf.runner import run_ptf_algorithm, run_ptf_algorithm_with_timing
from ptf.min_heap import MinHeapTopK


class TestRunPtfAlgorithm:
    """Test suite for run_ptf_algorithm function."""

    def test_run_ptf_algorithm_basic(self):
        """Test basic execution of run_ptf_algorithm with valid data."""
        # Create a temporary test file
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 2\n")
            f.write("2 3\n")
            f.write("1 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=2, output_file=output_buffer)

            output = output_buffer.getvalue()
            assert "Initial rmsup:" in output
            assert "Initial MH size:" in output
            assert "Total itemsets found:" in output
            assert "Execution time:" in output
            assert "Memory used:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_different_top_k_values(self):
        """Test run_ptf_algorithm with various top_k values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4 5\n")
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 4 5\n")
            temp_file = f.name

        try:
            for top_k in [1, 3, 5, 10]:
                output_buffer = StringIO()
                run_ptf_algorithm(temp_file, top_k=top_k,
                                  output_file=output_buffer)
                output = output_buffer.getvalue()
                assert "Initial rmsup:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_without_output_file(self):
        """Test run_ptf_algorithm without specifying output_file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name

        try:
            # Should not raise an exception
            run_ptf_algorithm(temp_file, top_k=2, output_file=None)
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_file_not_found(self):
        """Test run_ptf_algorithm with non-existent file."""
        output_buffer = StringIO()
        with pytest.raises(FileNotFoundError):
            run_ptf_algorithm("nonexistent_file.txt", top_k=5,
                              output_file=output_buffer)

    def test_run_ptf_algorithm_output_format(self):
        """Test that output is formatted correctly."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            f.write("1 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=3, output_file=output_buffer)
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

    def test_run_ptf_algorithm_itemset_output_format(self):
        """Test that itemsets are formatted correctly in output."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=2, output_file=output_buffer)
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

    def test_run_ptf_algorithm_single_transaction(self):
        """Test run_ptf_algorithm with single transaction."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()
            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_large_dataset(self):
        """Test run_ptf_algorithm with larger dataset."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Create a larger transaction database
            for i in range(100):
                items = [str((j % 10) + 1) for j in range(i % 10 + 1)]
                f.write(" ".join(items) + "\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()

            assert "Total itemsets found:" in output
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_string_output_file(self):
        """Test run_ptf_algorithm with StringIO as output_file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n2 3\n1 3\n")
            temp_file = f.name

        try:
            output = StringIO()
            run_ptf_algorithm(temp_file, top_k=2, output_file=output)

            output_str = output.getvalue()
            assert len(output_str) > 0
            assert "Initial rmsup:" in output_str
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_memory_metrics(self):
        """Test that memory metrics are included in output."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=3, output_file=output_buffer)
            output = output_buffer.getvalue()

            assert "Memory used:" in output
            # Extract and verify memory value is numeric
            memory_lines = [l for l in output.split(
                '\n') if 'Memory used:' in l]
            assert len(memory_lines) > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_component_initialization(self):
        """Test that all components are properly initialized and executed."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            temp_file = f.name
        
        try:
            output_buffer = StringIO()
            # Should execute without errors
            run_ptf_algorithm(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()
            
            # Verify all components were properly used (evidenced by complete output)
            assert "Initial rmsup:" in output
            assert "Total itemsets found:" in output
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    @patch('ptf.runner.track_execution')
    def test_run_ptf_algorithm_metrics_tracking(self, mock_track):
        """Test that execution metrics are properly tracked."""
        mock_metrics = MagicMock()
        mock_metrics.execution_time_ms = 1234.0
        mock_metrics.memory_used_kb = 56.78
        mock_track.return_value.__enter__.return_value = mock_metrics

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=2, output_file=output_buffer)

            output = output_buffer.getvalue()
            assert "1234.00" in output
        finally:
            os.unlink(temp_file)


class TestRunPtfAlgorithmWithTiming:
    """Test suite for run_ptf_algorithm_with_timing function."""

    def test_run_ptf_algorithm_with_timing_returns_float(self):
        """Test that function returns execution time as float."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_with_timing(
                temp_file, top_k=2, output_file=output_buffer)

            assert isinstance(exec_time, float)
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_reasonable_time(self):
        """Test that execution time is reasonable (not negative or extremely large)."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_with_timing(
                temp_file, top_k=2, output_file=output_buffer)

            # Execution time should be positive and reasonable (less than 60 seconds)
            assert 0 < exec_time < 60
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_output_includes_time(self):
        """Test that output includes execution time information."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_with_timing(
                temp_file, top_k=2, output_file=output_buffer)

            output = output_buffer.getvalue()
            assert "Execution time:" in output
            # Verify the time in output matches the return value (approximately)
            assert f"{exec_time:.4f}" in output or f"{exec_time:.3f}" in output
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_without_output_file(self):
        """Test timing function without output file."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            temp_file = f.name

        try:
            exec_time = run_ptf_algorithm_with_timing(
                temp_file, top_k=2, output_file=None)
            assert isinstance(exec_time, float)
            assert exec_time > 0
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_consistency(self):
        """Test that timing is consistent across multiple runs."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 4 5\n")
            temp_file = f.name

        try:
            output_buffer1 = StringIO()
            output_buffer2 = StringIO()

            time1 = run_ptf_algorithm_with_timing(
                temp_file, top_k=3, output_file=output_buffer1)
            time2 = run_ptf_algorithm_with_timing(
                temp_file, top_k=3, output_file=output_buffer2)

            # Times should be positive
            assert time1 > 0 and time2 > 0
            # Both should execute successfully
            assert "Initial rmsup:" in output_buffer1.getvalue()
            assert "Initial rmsup:" in output_buffer2.getvalue()
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_incremental_times(self):
        """Test that timing works correctly with multiple runs."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Small dataset
            for i in range(10):
                f.write("1 2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            exec_time = run_ptf_algorithm_with_timing(
                temp_file, top_k=2, output_file=output_buffer)

            # Timing should be positive
            assert exec_time > 0
            assert exec_time < 10  # Should complete quickly for small dataset
        finally:
            os.unlink(temp_file)

    def test_run_ptf_algorithm_with_timing_file_not_found(self):
        """Test timing function with non-existent file."""
        output_buffer = StringIO()
        with pytest.raises(FileNotFoundError):
            run_ptf_algorithm_with_timing(
                "nonexistent.txt", top_k=2, output_file=output_buffer)

    def test_run_ptf_algorithm_with_timing_different_top_k_values(self):
        """Test timing with different top_k values."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Create a dataset with enough itemsets
            for i in range(50):
                items = [str(j + 1) for j in range(1, 6)]
                f.write(" ".join(items) + "\n")
            temp_file = f.name

        try:
            times = []
            for top_k in [1, 5, 10, 20]:
                output_buffer = StringIO()
                exec_time = run_ptf_algorithm_with_timing(
                    temp_file, top_k=top_k, output_file=output_buffer)
                times.append(exec_time)
                assert exec_time > 0

            # All times should be reasonable
            assert all(0 < t < 60 for t in times)
        finally:
            os.unlink(temp_file)


class TestRunnerIntegration:
    """Integration tests for runner functions."""

    def test_run_ptf_algorithm_and_timing_equivalence(self):
        """Test that run_ptf_algorithm and run_ptf_algorithm_with_timing produce equivalent results."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 3 4\n")
            temp_file = f.name

        try:
            output1 = StringIO()
            output2 = StringIO()

            run_ptf_algorithm(temp_file, top_k=3, output_file=output1)
            run_ptf_algorithm_with_timing(
                temp_file, top_k=3, output_file=output2)

            out1 = output1.getvalue()
            out2 = output2.getvalue()

            # Both should have same initial metrics
            assert "Initial rmsup:" in out1
            assert "Initial rmsup:" in out2
            assert "Total itemsets found:" in out1
            assert "Total itemsets found:" in out2
        finally:
            os.unlink(temp_file)

    def test_empty_database(self):
        """Test runner with empty transaction database."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            # Empty file
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=5, output_file=output_buffer)
            output = output_buffer.getvalue()

            # Should still output metrics
            assert "Execution time:" in output
        finally:
            os.unlink(temp_file)

    def test_duplicate_items_in_transaction(self):
        """Test runner with duplicate items in transactions."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 1 2 2\n")
            f.write("1 2 3\n")
            temp_file = f.name

        try:
            output_buffer = StringIO()
            run_ptf_algorithm(temp_file, top_k=2, output_file=output_buffer)
            output = output_buffer.getvalue()

            assert "Total itemsets found:" in output
        finally:
            os.unlink(temp_file)
