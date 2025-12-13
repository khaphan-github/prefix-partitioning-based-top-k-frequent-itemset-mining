from ptf.transaction_db import TransactionDB
import pytest
import tempfile
import os


class TestTransactionDB:
    """Test suite for TransactionDB class"""

    @pytest.fixture
    def temp_transaction_file(self):
        """Create a temporary transaction file for testing"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("2 3 4\n")
            f.write("1 4 5\n")
            temp_file = f.name
        yield temp_file
        os.unlink(temp_file)

    @pytest.fixture
    def temp_file_with_blanks(self):
        """Create a temporary file with blank lines"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("\n")
            f.write("4 5\n")
            f.write("   \n")
            f.write("1 2 6\n")
            temp_file = f.name
        yield temp_file
        os.unlink(temp_file)

    @pytest.fixture
    def temp_file_unsorted(self):
        """Create a temporary file with unsorted items"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("5 2 8\n")
            f.write("3 1 4\n")
            f.write("7 2 1\n")
            temp_file = f.name
        yield temp_file
        os.unlink(temp_file)

    # Test 1: load_transactions - Valid file loading
    def test_load_transactions_valid_file(self, temp_transaction_file):
        """Test loading transactions from a valid file"""
        db = TransactionDB(temp_transaction_file)

        assert len(db.transactions) == 3
        assert db.transactions[0] == [1, 2, 3]
        assert db.transactions[1] == [2, 3, 4]
        assert db.transactions[2] == [1, 4, 5]
        assert db.all_items == {1, 2, 3, 4, 5}

    # Test 2: load_transactions - Blank lines are skipped
    def test_load_transactions_with_blank_lines(self, temp_file_with_blanks):
        """Test that blank lines are properly skipped"""
        db = TransactionDB(temp_file_with_blanks)

        assert len(db.transactions) == 3
        assert db.transactions[0] == [1, 2, 3]
        assert db.transactions[1] == [4, 5]
        assert db.transactions[2] == [1, 2, 6]
        assert db.all_items == {1, 2, 3, 4, 5, 6}

    # Test 3: load_transactions - Items are sorted
    def test_load_transactions_sorts_items(self, temp_file_unsorted):
        """Test that items within each transaction are sorted"""
        db = TransactionDB(temp_file_unsorted)

        assert db.transactions[0] == [2, 5, 8]  # [5, 2, 8] sorted
        assert db.transactions[1] == [1, 3, 4]  # [3, 1, 4] sorted
        assert db.transactions[2] == [1, 2, 7]  # [7, 2, 1] sorted

    # Test 4: __init__ - Proper initialization
    def test_init_proper_initialization(self, temp_transaction_file):
        """Test that __init__ properly initializes TransactionDB"""
        db = TransactionDB(temp_transaction_file)

        assert db.file_path == temp_transaction_file
        assert isinstance(db.transactions, list)
        assert isinstance(db.all_items, set)
        assert len(db.transactions) > 0
        assert len(db.all_items) > 0

    # Test 5: __init__ - File not found exception
    def test_init_file_not_found(self):
        """Test that FileNotFoundError is raised for non-existent file"""
        with pytest.raises(FileNotFoundError):
            TransactionDB("/nonexistent/path/to/file.txt")

    # Test 6: load_transactions - Invalid data format
    def test_load_transactions_invalid_format(self):
        """Test that ValueError is raised for invalid data format"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 3\n")
            f.write("1 abc 3\n")  # Invalid: 'abc' cannot be converted to int
            temp_file = f.name

        try:
            with pytest.raises(ValueError, match="Invalid format on line 2"):
                TransactionDB(temp_file)
        finally:
            os.unlink(temp_file)

    # Test 7: load_transactions - Single item transactions
    def test_load_transactions_single_items(self):
        """Test transactions with single items"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1\n")
            f.write("5\n")
            f.write("3\n")
            temp_file = f.name

        try:
            db = TransactionDB(temp_file)
            assert len(db.transactions) == 3
            assert db.transactions == [[1], [5], [3]]
            assert db.all_items == {1, 3, 5}
        finally:
            os.unlink(temp_file)

    # Test 8: load_transactions - Duplicate items in transaction
    def test_load_transactions_duplicate_items(self):
        """Test that duplicate items in transactions are handled"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1 2 2 3\n")
            f.write("1 1 1\n")
            temp_file = f.name

        try:
            db = TransactionDB(temp_file)
            # Duplicates preserved in transaction
            assert db.transactions[0] == [1, 2, 2, 3]
            assert db.transactions[1] == [1, 1, 1]
            assert db.all_items == {1, 2, 3}  # Set removes duplicates
        finally:
            os.unlink(temp_file)

    # Test 9: load_transactions - Large numbers
    def test_load_transactions_large_numbers(self):
        """Test handling of large item numbers"""
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write("1000 5000 999999\n")
            f.write("2000 3000\n")
            temp_file = f.name

        try:
            db = TransactionDB(temp_file)
            assert db.transactions[0] == [1000, 5000, 999999]
            assert db.all_items == {1000, 2000, 3000, 5000, 999999}
        finally:
            os.unlink(temp_file)

    # Test 10: load_transactions - Return values
    def test_load_transactions_return_values(self, temp_transaction_file):
        """Test that load_transactions returns correct tuple"""
        db = TransactionDB(temp_transaction_file)
        transactions, all_items = db.load_transactions(temp_transaction_file)

        assert transactions == db.transactions
        assert all_items == db.all_items
        assert isinstance(transactions, list)
        assert isinstance(all_items, set)
