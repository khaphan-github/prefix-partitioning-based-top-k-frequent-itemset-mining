from ptf.prefix_partitioning import PrefixPartitioning
from ptf.transaction_db import TransactionDB
import pytest
import tempfile
import os


class TestCreatePrefixPartitions:
    """Unit tests for PrefixPartitioning.create_prefix_partitions()"""

    @pytest.fixture
    def sample_db(self):
        """Create a temporary transaction file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 3 4\n")
            f.write("2 3\n")
            f.write("1 2\n")
            temp_file = f.name

        db = TransactionDB(temp_file)
        yield db
        os.unlink(temp_file)

    @pytest.fixture
    def single_item_db(self):
        """Create a transaction file with single-item transactions"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1\n")
            f.write("2\n")
            f.write("3\n")
            temp_file = f.name

        db = TransactionDB(temp_file)
        yield db
        os.unlink(temp_file)

    @pytest.fixture
    def empty_suffix_db(self):
        """Create a transaction file for testing empty suffixes"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2\n")
            f.write("2 3\n")
            temp_file = f.name

        db = TransactionDB(temp_file)
        yield db
        os.unlink(temp_file)

    def test_partition_keys_match_all_items(self, sample_db):
        """Test that partition keys contain all items"""
        partitioner = PrefixPartitioning(sample_db)
        partitions = partitioner.prefix_partitions

        # Partition keys should be {1, 2, 3, 4}
        assert set(partitions.keys()) == {1, 2, 3, 4}

    def test_partition_structure(self, sample_db):
        """Test that partitions contain correct suffixes"""
        partitioner = PrefixPartitioning(sample_db)
        partitions = partitioner.prefix_partitions

        # For transaction [1, 2, 3]:
        # - suffix [1, 2, 3] goes to partition 1
        # For transaction [1, 3, 4]:
        # - suffix [1, 3, 4] goes to partition 1
        # For transaction [1, 2]:
        # - suffix [1, 2] goes to partition 1
        assert [1, 2, 3] in partitions[1]
        assert [1, 3, 4] in partitions[1]
        assert [1, 2] in partitions[1]

        # For transaction [1, 2, 3]:
        # - suffix [2, 3] goes to partition 2
        # For transaction [2, 3]:
        # - suffix [2, 3] goes to partition 2
        # For transaction [1, 2]:
        # - no suffix (empty) - should not add anything
        assert [2, 3] in partitions[2]

    def test_partition_completeness(self, sample_db):
        """Test that all transaction suffixes are captured exactly once"""
        partitioner = PrefixPartitioning(sample_db)
        partitions = partitioner.prefix_partitions

        # Count total suffixes across all partitions
        total_suffixes = sum(len(suffixes) for suffixes in partitions.values())

        # Original transactions: [1, 2, 3], [1, 3, 4], [2, 3], [1, 2]
        # Transaction [1, 2, 3]: partitions 1, 2, 3 (3 suffixes)
        # Transaction [1, 3, 4]: partitions 1, 3, 4 (3 suffixes)
        # Transaction [2, 3]: partitions 2, 3 (2 suffixes)
        # Transaction [1, 2]: partitions 1, 2 (2 suffixes)
        # Total: 3 + 3 + 2 + 2 = 10
        assert total_suffixes == 10

    def test_no_empty_suffixes_in_partitions(self, sample_db):
        """Test that empty suffixes are not added to partitions"""
        partitioner = PrefixPartitioning(sample_db)
        partitions = partitioner.prefix_partitions

        for partition_item, suffixes in partitions.items():
            for suffix in suffixes:
                assert len(
                    suffix) > 0, f"Empty suffix found in partition {partition_item}"

    def test_single_item_transactions(self, single_item_db):
        """Test edge case: single-item transactions"""
        partitioner = PrefixPartitioning(single_item_db)
        partitions = partitioner.prefix_partitions

        # Each transaction [x] creates one suffix [x] for partition x
        assert [1] in partitions[1]
        assert [2] in partitions[2]
        assert [3] in partitions[3]
        assert len(partitions[1]) == 1
        assert len(partitions[2]) == 1
        assert len(partitions[3]) == 1

    def test_partition_suffix_correctness(self, empty_suffix_db):
        """Test that suffixes are correctly extracted"""
        partitioner = PrefixPartitioning(empty_suffix_db)
        partitions = partitioner.prefix_partitions

        # Transaction [1, 2] creates suffixes:
        # - [1, 2] for partition 1
        # - nothing for partition 2 (empty suffix not added)
        assert [1, 2] in partitions[1]
        # Transaction [2, 3] creates:
        # - [2, 3] for partition 2
        # - [3] for partition 3
        assert [2, 3] in partitions[2]
        assert [3] in partitions[3]

    def test_partition_item_order_preserved(self, sample_db):
        """Test that item order within suffixes is preserved"""
        partitioner = PrefixPartitioning(sample_db)
        partitions = partitioner.prefix_partitions

        # All suffixes should maintain sorted order (same as input)
        for partition_item, suffixes in partitions.items():
            for suffix in suffixes:
                assert suffix == sorted(
                    suffix), f"Suffix {suffix} is not sorted in partition {partition_item}"

    def test_partition_attributes(self, sample_db):
        """Test that PrefixPartitioning object has expected attributes"""
        partitioner = PrefixPartitioning(sample_db)

        assert hasattr(partitioner, 'transactions_db')
        assert hasattr(partitioner, 'prefix_partitions')
        assert isinstance(partitioner.prefix_partitions, dict)
        assert partitioner.transactions_db == sample_db

    def test_large_transaction(self):
        """Test with a large transaction"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3 4 5 6 7 8 9 10\n")
            temp_file = f.name

        try:
            db = TransactionDB(temp_file)
            partitioner = PrefixPartitioning(db)
            partitions = partitioner.prefix_partitions

            # Transaction [1,2,3,4,5,6,7,8,9,10] creates 10 suffixes (one per partition)
            total_suffixes = sum(len(suffixes)
                                 for suffixes in partitions.values())
            assert total_suffixes == 10

            # All items should be partition keys
            assert set(partitions.keys()) == set(range(1, 11))
        finally:
            os.unlink(temp_file)

    def test_duplicate_items_in_transaction(self):
        """Test behavior with sorted items (no duplicates expected in real data)"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 2 3\n")  # This would be sorted to [1, 2, 2, 3]
            temp_file = f.name

        try:
            db = TransactionDB(temp_file)
            partitioner = PrefixPartitioning(db)
            partitions = partitioner.prefix_partitions

            # Should still work with duplicates
            assert 1 in partitions
            assert [1, 2, 2, 3] in partitions[1]
        finally:
            os.unlink(temp_file)

    def test_initialization(self, sample_db):
        """Test that PrefixPartitioning initializes correctly"""
        partitioner = PrefixPartitioning(sample_db)

        # Partitions should be created immediately upon initialization
        assert partitioner.prefix_partitions is not None
        assert len(partitioner.prefix_partitions) > 0


class TestCoNStructure:
    """Placeholder for future CoN structure tests"""
    pass
