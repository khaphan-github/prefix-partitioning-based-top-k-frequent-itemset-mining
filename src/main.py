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

    # Main algorithm
    ptf = PrefixPartitioningbasedTopKAlgorithm(top_k=8)
    min_heap, rmsup = ptf.initialize_mh_and_rmsup(
        co_occurrence_numbers.full_co_occurrence_list)

    assert rmsup == 3
    assert len(min_heap.heap) == 8

    promissing_arr = ptf.build_promissing_item_arrays(
        min_heap=min_heap,
        all_items=db.all_items,
    )

    partitions_to_process = ptf.filter_partitions(
        ar=promissing_arr,
        all_items=db.all_items,
        con_map=co_occurrence_numbers.con_map,
        min_heap=min_heap,
        rmsup=rmsup
    )

    print("Partitions to process:", partitions_to_process)
    print("Final results:", min_heap.get_all())
