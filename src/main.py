from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm


if __name__ == "__main__":
    '''
    - Read transaction database
    - Create prefix-based partitions
    - Print summary of partitions
    - 
    '''

    # Read transaction database
    db = TransactionDB("data/sample.txt")
    partitioner = PrefixPartitioning(db)
    co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)

    ptf = PrefixPartitioningbasedTopKAlgorithm(top_k=8)
    min_heap, rmsup = ptf.initialize_mh_and_rmsup(
        co_occurrence_numbers.full_co_occurrence_list)

    assert rmsup == 3
    assert len(min_heap.heap) == 8