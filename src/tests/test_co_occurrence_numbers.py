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

        # Note: prefix in result is transaction[0], not the loop's prefix key
        assert 2 in result
        assert result[2] == {2: 2, 3: 1, 4: 1}

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
        # Prefix is transaction[0]
        assert result[2] == {2: 2, 3: 1, 4: 1}
        assert result[3] == {3: 2, 4: 1}
        assert result[4] == {4: 1}

    def test_build_partition_con_empty_transactions(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with empty transactions in partition."""
        mock_prefix_partition.prefix_partitions = {
            1: [[], [2, 3], []]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        # Empty transactions should be skipped. Prefix is transaction[0]
        assert result[2] == {2: 1, 3: 1}

    def test_build_partition_con_single_item_transactions(self, mock_prefix_partition, mock_transaction_db):
        """Test _build_partition_con with single item transactions."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2], [3], [2]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        # Results are keyed by the LAST transaction[0]
        # First transaction [2] -> prefix 2
        # Second transaction [3] -> prefix 3 (overwrites)
        # Third transaction [2] -> prefix 2 (overwrites)
        # Final key is 2 from the last transaction
        assert 2 in result
        assert result[2][2] == 2  # item 2 appears in trans 1 and 3
        assert result[2][3] == 1  # item 3 appears in trans 2

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

        result_dict, result_list = co_occ._merge_partition_con(
            partition_con_dict)

        # Result should be a dict with prefix as key
        assert isinstance(result_dict, dict)
        assert 1 in result_dict

        # Check the list of (itemset, count) tuples
        con_list = result_dict[1]
        assert len(con_list) == 3

        # Verify items are sorted by count (descending)
        counts = [count for _, count in con_list]
        assert counts == [3, 2, 1]

        # Verify itemsets are sets
        itemsets = [itemset for itemset, _ in con_list]
        assert {1} in itemsets
        assert {1, 2} in itemsets
        assert {1, 3} in itemsets

    def test_merge_partition_con_multiple_prefixes(self, mock_prefix_partition, mock_transaction_db):
        """Test _merge_partition_con with multiple prefixes."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        partition_con_dict = {
            1: {1: 2, 2: 3},
            2: {2: 1, 3: 4}
        }

        result_dict, result_list = co_occ._merge_partition_con(
            partition_con_dict)

        # Result should be a dict
        assert isinstance(result_dict, dict)
        assert len(result_dict) == 2

        # Check prefix 1
        assert 1 in result_dict
        con_list_1 = result_dict[1]
        counts_1 = [count for _, count in con_list_1]
        assert counts_1 == sorted(counts_1, reverse=True)

        # Check prefix 2
        assert 2 in result_dict
        con_list_2 = result_dict[2]
        counts_2 = [count for _, count in con_list_2]
        assert counts_2 == sorted(counts_2, reverse=True)

    def test_merge_partition_con_empty_dict(self, mock_prefix_partition, mock_transaction_db):
        """Test _merge_partition_con with empty dictionary."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        result_dict, result_list = co_occ._merge_partition_con({})

        assert isinstance(result_dict, dict)
        assert result_dict == {}
        assert result_list == []

    def test_compute_co_occurrence_numbers_single_partition(self, mock_prefix_partition, mock_transaction_db):
        """Test compute_co_occurrence_numbers with single partition."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3], [2, 4]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        # Result should be a dict keyed by prefix
        assert isinstance(co_occ.co_occurrence_numbers, dict)
        assert len(co_occ.co_occurrence_numbers) > 0

        # Check that results within each prefix are sorted by count (descending)
        for prefix, con_list in co_occ.co_occurrence_numbers.items():
            counts = [count for _, count in con_list]
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

        # Check that result is a dict
        assert isinstance(result, dict)

        # Check that results within each prefix are sorted by count in descending order
        for prefix, con_list in result.items():
            counts = [count for _, count in con_list]
            assert counts == sorted(counts, reverse=True)

            # Check that all results are tuples of (set, int)
            for itemset, count in con_list:
                assert isinstance(itemset, set)
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

        # Verify each prefix's items are sorted by count (descending)
        for prefix, con_list in result.items():
            counts = [count for _, count in con_list]
            assert counts == sorted(counts, reverse=True)

    def test_co_occurrence_numbers_attribute_set(self, mock_prefix_partition, mock_transaction_db):
        """Test that co_occurrence_numbers attribute is set during initialization."""
        mock_prefix_partition.prefix_partitions = {
            1: [[2, 3]]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        assert hasattr(co_occ, 'co_occurrence_numbers')
        assert isinstance(co_occ.co_occurrence_numbers, dict)

    def test_build_partition_con_counts_accurately(self, mock_prefix_partition, mock_transaction_db):
        """Test that _build_partition_con counts items accurately."""
        mock_prefix_partition.prefix_partitions = {
            'a': [['b', 'c', 'd'], ['b', 'e'], ['c', 'e'], ['b']]
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)
        result = co_occ._build_partition_con()

        # The final prefix key is the LAST transaction[0], which is 'b'
        # All items in con_i accumulate across all transactions
        # b appears in trans 0, 1, 3 = 3 times
        # c appears in trans 0, 2 = 2 times
        # d appears in trans 0 = 1 time
        # e appears in trans 1, 2 = 2 times
        assert 'b' in result
        assert result['b']['b'] == 3
        assert result['b']['c'] == 2
        assert result['b']['d'] == 1
        assert result['b']['e'] == 2

    def test_merge_partition_con_frozenset_creation(self, mock_prefix_partition, mock_transaction_db):
        """Test that _merge_partition_con correctly creates sets."""
        mock_prefix_partition.prefix_partitions = {}

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        partition_con_dict = {
            'x': {'x': 5, 'y': 3, 'z': 2}
        }

        result_dict, result_list = co_occ._merge_partition_con(
            partition_con_dict)

        # Check set structure
        assert 'x' in result_dict
        con_list = result_dict['x']
        itemsets = [itemset for itemset, _ in con_list]

        assert {'x'} in itemsets
        assert {'x', 'y'} in itemsets
        assert {'x', 'z'} in itemsets

    def test_empty_all_partitions(self, mock_prefix_partition, mock_transaction_db):
        """Test with all empty partitions."""
        mock_prefix_partition.prefix_partitions = {
            1: [],
            2: [],
            3: []
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        assert isinstance(co_occ.co_occurrence_numbers, dict)
        assert co_occ.co_occurrence_numbers == {}

    def test_large_dataset_simulation(self, mock_prefix_partition, mock_transaction_db):
        """Test with a larger simulated dataset."""
        mock_prefix_partition.prefix_partitions = {
            i: [[j for j in range(i + 1, i + 5)] for _ in range(10)]
            for i in range(1, 11)
        }

        co_occ = CoOccurrenceNumbers(
            mock_prefix_partition, mock_transaction_db)

        # Verify that co_occurrence_numbers is a dict
        assert isinstance(co_occ.co_occurrence_numbers, dict)
        assert len(co_occ.co_occurrence_numbers) > 0

        # Verify sorting within each prefix
        for prefix, con_list in co_occ.co_occurrence_numbers.items():
            counts = [count for _, count in con_list]
            assert counts == sorted(counts, reverse=True)
