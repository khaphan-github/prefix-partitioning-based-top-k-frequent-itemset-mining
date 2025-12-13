from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers


if __name__ == "__main__":
    '''
    - Read transaction database
    - Create prefix-based partitions
    - Print summary of partitions
    '''

    # Read transaction database
    db = TransactionDB("data/sample.txt")
    partitioner = PrefixPartitioning(db)
    co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)
    print(co_occurrence_numbers.to_string())