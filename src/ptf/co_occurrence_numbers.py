'''
- Từ các phân vùng:
- Đếm số  so lan xuat hien tiem x tren cac phan vung
- Merge các elemetn lai
- sorted in the descending order of count. (Khong phai support)
'''

from typing import Dict, Tuple
from ptf.prefix_partitioning import PrefixPartitioning
from ptf.transaction_db import TransactionDB


class CoOccurrenceNumbers:
    def __init__(self, prefix_partition: PrefixPartitioning, transaction_db: TransactionDB):
        self.prefix_partition = prefix_partition
        self.transaction_db = transaction_db
        self.co_occurrence_numbers, self.full_co_occurrence_list = self.compute_co_occurrence_numbers()
        self.con_map = self._build_con_map()

    def compute_co_occurrence_numbers(self):
        partition_con_dict = self._build_partition_con()
        return self._merge_partition_con(partition_con_dict)

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
        output: [ prefix: [(itemset, count), ...]]}
        '''

        CoN: Dict[int, Tuple[int, int]] = {}

        for prefix, con_i in partition_con_dict.items():
            for item, count in con_i.items():
                if item == prefix:
                    itemset = set([prefix])
                else:
                    itemset = set([prefix, item])

                if prefix not in CoN:
                    CoN[prefix] = []
                CoN[prefix].append((itemset, count))

        # Orderrihg each items in descending order of count
        # x[1] la count
        for prefix, con_list in CoN.items():
            con_list.sort(key=lambda x: x[1], reverse=True)
            CoN[prefix] = con_list

        full_con_list = []
        for prefix, con_list in CoN.items():
            full_con_list.extend(con_list)
        full_con_list.sort(key=lambda x: x[1], reverse=True)
        return CoN, full_con_list

    def _build_con_map(self):
        '''
        Build a fast lookup map: frozenset(itemset) -> support
        Used in filter_partitions for O(1) 2-itemset support lookup

        Returns:
            Dict[frozenset, int]: Maps itemset frozensets to their support values
        '''
        con_map = {}
        for itemset, support in self.full_co_occurrence_list:
            con_map[frozenset(itemset)] = support
        return con_map

    def to_string(self):
        result = ""
        for prefix, con_list in self.co_occurrence_numbers.items():
            result += f"Prefix {prefix}:\n"
            for itemset, count in con_list:
                result += f"Item {itemset}: Count {count}\n"
        return result
