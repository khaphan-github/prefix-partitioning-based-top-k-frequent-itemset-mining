import pytest
from unittest.mock import Mock, MagicMock
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.transaction_db import TransactionDB


class TestCoOccurrenceNumbers:
    """Test suite for CoOccurrenceNumbers class."""

    @pytest.fixture
    def mock_transaction_db(self):
        """Create a mock TransactionDB."""
        mock_db = Mock(spec=TransactionDB)
        return mock_db

    @pytest.fixture
    def mock_prefix_partition(self):
        """Create a mock PrefixPartitioning."""
        mock_partition = Mock(spec=PrefixPartitioning)
        return mock_partition

    def test_initialization(self, mock_prefix_partition, mock_transaction_db):
        """Test that CoOccurrenceNumbers initializes correctly."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        assert co_occ.prefix_partition == mock_prefix_partition
        assert co_occ.transaction_db == mock_transaction_db
        assert co_occ.co_occurrence_numbers is not None

    def test_build_partition_con_single_partition(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with a single partition."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3], [2, 4]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        assert 1 in result
        assert result[1] == {2: 2, 3: 1, 4: 1}

    def test_build_partition_con_multiple_partitions(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with multiple partitions."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3], [2, 4]],
            2: [[3], [3, 4]],
            3: [[4], []]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        assert len(result) == 3
        assert result[1] == {2: 2, 3: 1, 4: 1}
        assert result[2] == {3: 2, 4: 1}
        assert result[3] == {4: 1}

    def test_build_partition_con_empty_transactions(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with empty transactions in partition."""
        mock_prefix_partition.prefix_partitions = {
            1: [[], [2, 3], []]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        # Empty transactions should be skipped
        assert result[1] == {2: 1, 3: 1}

    def test_build_partition_con_single_item_transactions(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with single item transactions."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2], [3], [2]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        assert result[1] == {2: 2, 3: 1}

    def test_build_partition_con_empty_partition(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with empty partition."""
        mock_prefix_partition.prefix_partitions = {
            1: []
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        assert result[1] == {}

    def test_merge_partition_con_single_prefix(self, mock_prefix_partition, mock_transaction_db):
        """Test _merge_partition_con with single prefix."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        partition_con_dict = {
            1: {2: 3, 3: 2, 1: 1}
        }

        result = co_occ._merge_partition_con(partition_con_dict)

        # Convert to set for easier comparison
        result_set = {(itemset, count) for itemset, count in result}

        expected = {
            (frozenset([1]), 1),
            (frozenset([1, 2]), 3),
            (frozenset([1, 3]), 2)
        }

        assert result_set == expected

    def test_merge_partition_con_multiple_prefixes(self, mock_prefix_partition, mock_transaction_db):
        """Test _merge_partition_con with multiple prefixes."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        partition_con_dict = {
            1: {1: 2, 2: 3},
            2: {2: 1, 3: 4}
        }

        result = co_occ._merge_partition_con(partition_con_dict)

        result_set = {(itemset, count) for itemset, count in result}

        expected = {
            (frozenset([1]), 2),
            (frozenset([1, 2]), 3),
            (frozenset([2]), 1),
            (frozenset([2, 3]), 4)
        }

        assert result_set == expected

    def test_merge_partition_con_empty_dict(self, mock_prefix_partition, mock_transaction_db):
        """Test _merge_partition_con with empty dictionary."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        result = co_occ._merge_partition_con({})

        assert result == []

    def test_compute_co_occurrence_numbers_single_partition(self, mock_prefix_partition, mock_transaction_db):
        """Test compute_co_occurrence_numbers with single partition."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3], [2, 4]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        # Result should be sorted in descending order by count
        assert len(co_occ.co_occurrence_numbers) > 0

        # Check that results are sorted by count (second element) in descending order
        counts = [count for _, count in co_occ.co_occurrence_numbers]
        assert counts == sorted(counts, reverse=True)

    def test_compute_co_occurrence_numbers_multiple_partitions(self, mock_prefix_partition, mock_transaction_db):
        """Test compute_co_occurrence_numbers with multiple partitions."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3], [2, 4]],
            2: [[3], [3, 4]],
            3: [[4]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        result = co_occ.co_occurrence_numbers

        # Check that results are sorted by count in descending order
        counts = [count for _, count in result]
        assert counts == sorted(counts, reverse=True)

        # Check that all results are tuples of (frozenset, int)
        for itemset, count in result:
            assert isinstance(itemset, frozenset)
            assert isinstance(count, int)

    def test_compute_co_occurrence_numbers_sorting(self, mock_prefix_partition, mock_transaction_db):
        """Test that compute_co_occurrence_numbers correctly sorts by count."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2], [2], [2], [3]],  # 2 appears 3 times, 3 appears 1 time
            2: [[3, 4], [3, 4], [4]]   # 3 appears 2 times, 4 appears 3 times
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ.co_occurrence_numbers

        # Extract counts in order
        counts = [count for _, count in result]

        # Verify descending order
        assert counts == sorted(counts, reverse=True)

    def test_co_occurrence_numbers_attribute_set(self, mock_prefix_partition, mock_transaction_db):
        """Test that co_occurrence_numbers attribute is set during initialization."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        assert hasattr(co_occ, 'co_occurrence_numbers')
        assert isinstance(co_occ.co_occurrence_numbers, list)

    def test_build_partition_con_counts_accurately(self, mock_prefix_partition, mock_transaction_db):
        """Test that _build_partition_con counts items accurately."""
        mock_prefix_partition.prefix_partitions = {
            'a': [['b', 'c', 'd'], ['b', 'e'], ['c', 'e'], ['b']]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        # b appears in first, second, and fourth transactions = 3 times
        # c appears in first and third transactions = 2 times
        # d appears in first transaction = 1 time
        # e appears in second and third transactions = 2 times
        assert result['a']['b'] == 3
        assert result['a']['c'] == 2
        assert result['a']['d'] == 1
        assert result['a']['e'] == 2

    def test_merge_partition_con_frozenset_creation(self, mock_prefix_partition, mock_transaction_db):
        """Test that _merge_partition_con correctly creates frozensets."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        partition_con_dict = {
            'x': {'x': 5, 'y': 3, 'z': 2}
        }

        result = co_occ._merge_partition_con(partition_con_dict)

        # Check frozenset structure
        itemsets = [itemset for itemset, _ in result]

        assert frozenset(['x']) in itemsets
        assert frozenset(['x', 'y']) in itemsets
        assert frozenset(['x', 'z']) in itemsets

    def test_empty_all_partitions(self, mock_prefix_partition, mock_transaction_db):
        """Test with all empty partitions."""
        mock_prefix_partition.prefix_partitions = {
            1: [],
            2: [],
            3: []
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        assert co_occ.co_occurrence_numbers == []

    def test_large_dataset_simulation(self, mock_prefix_partition, mock_transaction_db):
        """Test with a larger simulated dataset."""
        mock_prefix_partition.prefix_partitions = {
            i: [[j for j in range(i + 1, i + 5)] for _ in range(10)]
            for i in range(1, 11)
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        # Verify that co_occurrence_numbers is a sorted list
        assert isinstance(co_occ.co_occurrence_numbers, list)
        assert len(co_occ.co_occurrence_numbers) > 0

        # Verify sorting
        counts = [count for _, count in co_occ.co_occurrence_numbers]
        assert counts == sorted(counts, reverse=True)
