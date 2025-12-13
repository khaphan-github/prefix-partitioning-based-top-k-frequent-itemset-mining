from ptf.transaction_db import TransactionDB
from ptf.prefix_partitioning import PrefixPartitioning


if __name__ == "__main__":
    '''
    - Read transaction database
    - Create prefix-based partitions
    - Print summary of partitions
    '''

    # Read transaction database
    db = TransactionDB("data/sample.txt")
    partitioner = PrefixPartitioning(db)
    partitions = partitioner.prefix_partitions


    for prefix in sorted(partitions.keys()):
        suffixes = partitions[prefix]
        print(f"Partition {prefix}: {len(suffixes)} suffixes")
        for suffix in suffixes:  # Show first 3 suffixes
            print(f"  → {suffix}")