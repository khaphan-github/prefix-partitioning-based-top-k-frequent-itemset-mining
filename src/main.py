from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm


if __name__ == "__main__":
    '''
    - Read transaction database
    - Create prefix-based partitions
    - Run PTF algorithm
    - Export final results
    '''

    # Read transaction database
    db = TransactionDB("data/sample.txt")
    partitioner = PrefixPartitioning(db)
    co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)

    # Main algorithm
    ptf = PrefixPartitioningbasedTopKAlgorithm(top_k=8)
    min_heap, rmsup = ptf.initialize_mh_and_rmsup(
        co_occurrence_numbers.full_co_occurrence_list)

    print("=" * 60)
    print("INITIAL STATE")
    print("=" * 60)
    print(f"Initial rmsup: {rmsup}")
    print(f"Initial MH size: {len(min_heap.heap)}")

    promissing_arr = ptf.build_promissing_item_arrays(
        min_heap=min_heap,
        all_items=db.all_items,
    )

    ptf.filter_partitions(
        promissing_arr=promissing_arr,
        partitions=db.all_items,
        min_heap=min_heap,
        con_map=co_occurrence_numbers.con_map,
        rmsup=rmsup,
        partitioner=partitioner
    )

    # Print final results
    print("\n" + "=" * 60)
    print("FINAL RESULTS (Top-k Frequent Itemsets)")
    print("=" * 60)

    final_results = min_heap.get_all()
    # Sort by support descending
    final_results.sort(key=lambda x: (-x[0], x[1]))

    print(f"\nTotal itemsets found: {len(final_results)}")
    print(f"Final rmsup: {min_heap.min_support()}\n")

    for rank, (support, itemset) in enumerate(final_results, 1):
        itemset_str = "{" + ", ".join(map(str, sorted(itemset))) + "}"
        print(f"{rank}. {itemset_str:20} => Support: {support}")

    print("\n" + "=" * 60)
