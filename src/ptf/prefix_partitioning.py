'''
if a transaction is [1, 2, 3], it creates:
Suffix [1, 2, 3] → partition for item 1
Suffix [2, 3] → partition for item 2
Suffix [3] → partition for item 3
'''

from ptf.transaction_db import TransactionDB


class PrefixPartitioning:
    def __init__(self, transactions_db: TransactionDB):
        self.transactions_db = transactions_db
        self.prefix_partitions = self.create_prefix_partitions()

    def create_prefix_partitions(self):
        prefix_partitions = {}

        for transaction in self.transactions_db.transactions:
            for i in range(len(transaction)):
                prefix = transaction[i]
                suffix = transaction[i:]

                if prefix not in prefix_partitions:
                    prefix_partitions[prefix] = []

                if suffix:
                    prefix_partitions[prefix].append(suffix)

        return prefix_partitions
    
    def to_string(self):
        result = ""
        for prefix, suffixes in self.prefix_partitions.items():
            result += f"Prefix: {prefix}\n"
            for suffix in suffixes:
                result += f"  Suffix: {suffix}\n"
        return result
