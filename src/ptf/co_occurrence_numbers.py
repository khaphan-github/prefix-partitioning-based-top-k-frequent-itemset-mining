'''
- Từ các phân vùng:
- Đếm số  so lan xuat hien tiem x tren cac phan vung
- Merge các elemetn lai
- sorted in the descending order of count. (Khong phai support)
'''

from ptf.prefix_partitioning import PrefixPartitioning
from ptf.transaction_db import TransactionDB


class CoOccurrenceNumbers:
    def __init__(self, prefix_partition: PrefixPartitioning, transaction_db: TransactionDB):
        self.prefix_partition = prefix_partition
        self.transaction_db = transaction_db
        self.co_occurrence_numbers = self.compute_co_occurrence_numbers()

    def compute_co_occurrence_numbers(self):
        partition_con_dict = self._build_partition_con()
        global_con = self._merge_partition_con(partition_con_dict)
        # NOTE: x[1] la xep theo cai count. descending
        sorted_con = sorted(global_con, key=lambda x: x[1], reverse=True)
        return sorted_con

    def _build_partition_con(self):
        '''
        Input: prefix partitions { prefix: [ [suffix1], [suffix2], ...], ...}
        Output: { prefix: {item: count, ...}, ...
        '''
        partition_con_dict = {}
        START_SUBFIX_INDEX = 1

        for prefix, partition in self.prefix_partition.prefix_partitions.items():
            con_i = {}

            for transaction in partition:
                if not transaction:
                    continue
                # NOTE:
                # Do bên prefix partitioning tác giả return [prefix, subfix1, subfix2, ...
                # Mên cho nay dem phai: get index 0 la prefix, con lai la subfix
                prefix = transaction[0]
                con_i[prefix] = con_i.get(prefix, 0) + START_SUBFIX_INDEX

                for j in range(1, len(transaction)):
                    subfix = transaction[j]
                    con_i[subfix] = con_i.get(subfix, 0) + START_SUBFIX_INDEX

            partition_con_dict[prefix] = con_i

        return partition_con_dict

    def _merge_partition_con(self, partition_con_dict):
        '''
        input: { prefix: {item: count, ...}, ...
        output: [(itemset, count), ...]}
        '''

        global_con = []

        for prefix, con_i in partition_con_dict.items():
            for item, count in con_i.items():
                if item == prefix:
                    itemset = set([prefix])
                else:
                    itemset = set([prefix, item])

                global_con.append((itemset, count))
        return global_con
