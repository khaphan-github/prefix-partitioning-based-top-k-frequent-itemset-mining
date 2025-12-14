from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.co_occurrence_numbers import CoOccurrenceNumbers
from ptf.algorithm import PrefixPartitioningbasedTopKAlgorithm


def run_ptf_algorithm(file_path: str, top_k: int = 8, output_file=None):
    """
    Helper function to run PTF algorithm with given parameters.
    Writes output to file if output_file is provided.
    """
    def write_output(text):
        if output_file:
            output_file.write(text + "\n")
        else:
            print(text)
    
    # Read transaction database
    db = TransactionDB(file_path)
    
    partitioner = PrefixPartitioning(db)
    co_occurrence_numbers = CoOccurrenceNumbers(partitioner, db)

    # Main algorithm
    ptf = PrefixPartitioningbasedTopKAlgorithm(top_k=top_k)
    min_heap, rmsup = ptf.initialize_mh_and_rmsup(
        co_occurrence_numbers.full_co_occurrence_list)

    write_output(f"Initial rmsup: {rmsup}")
    write_output(f"Initial MH size: {len(min_heap.heap)}")

    promissing_arr = ptf.build_promissing_item_arrays(
        min_heap=min_heap,
        all_items=db.all_items,
        con_map=co_occurrence_numbers.con_map,
        rmsup=rmsup
    )

    min_heap, rmsup = ptf.filter_partitions(
        promissing_arr=promissing_arr,
        partitions=db.all_items,
        min_heap=min_heap,
        con_map=co_occurrence_numbers.con_map,
        rmsup=rmsup,
        partitioner=partitioner
    )

    # Print final results
    final_results = min_heap.get_all()
    # Sort by support descending
    final_results.sort(key=lambda x: (-x[0], x[1]))

    write_output(f"\nTotal itemsets found: {len(final_results)}")
    write_output(f"Final rmsup: {min_heap.min_support()}\n")

    for rank, (support, itemset) in enumerate(final_results, 1):
        itemset_str = "{" + ", ".join(map(str, sorted(itemset))) + "}"
        write_output(f"{rank}. {itemset_str:20} => Support: {support}")

    write_output("\n" + "=" * 60)


if __name__ == "__main__":
    report_file = open("ptf_algorithm_report.txt", "w")
    
    test_cases = [
        (8, "Test Case 1: top_k=8"),
        (5, "Test Case 2: top_k=5"),
        (10, "Test Case 3: top_k=10"),
        (3, "Test Case 4: top_k=3"),
        (20, "Test Case 5: top_k=20"),
    ]
    
    for top_k, label in test_cases:
        report_file.write(f"\n{label}\n")
        report_file.write("-" * 40 + "\n")
        try:
            run_ptf_algorithm("data/sample.txt", top_k=top_k, output_file=report_file)
        except FileNotFoundError as e:
            report_file.write(f"Error: {e}\n")
    
    report_file.close()
    print("Report written to: ptf_algorithm_report.txt")
