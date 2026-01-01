from typing import Dict, Tuple
from joblib import Parallel, delayed
from collections import defaultdict

'''
https://github.com/joblib/loky
- Ky thuat: myultithreading: Chay tren 1 process (do bi GIL lock) -> dung shared memory giua cac thread nhung khong tang toc that su.
- Ky thuat: multitprocessing: 
  + Chay tren nhieu process (bypass GIL) 
  + moi process co vung nho rieng, Khi tao process moi no can copy data tu main process -> child process => cham o khau copy.
- Ky thuat dung loky - (joblib):
  + Co che tuong tu multiprocessing nhugn cai tien hon o khau copy data giua cac process.
  + No giai quyet van de nay bang cach tao 1 file mapping (memmap) (tao cai nay nhanh hon viec copy qua nhieu process)
    - cac process truy cap du lieu thong qua mapper nay (truy cap kieu truc tiep vao du lieu thogn qua mempap file nay)
    - Do dung co che contro (poitner)
    =>> Cai tien toc do dang ke.

Keyword: IPC (Inter-Process Communication), zero-copy IPC

??
Logi chicnh:
- Dem so luogn dong xuat hien cua cac item tren tung phan vung (prefix partition)
- Merge cac ket qua dem duoc
- Sap xep giam dan theo so luong dem duoc

?? XO xanh cac buoc IPC
Workflow:  process Main -> Process child.
- Main process có dữ liệu [1, 2, 3, 4].
- Main process gửi từng phần tử (hoặc batch) đến các worker process.
- worker process thực hiện tính.
- Main process nhận kết quả và hợp lại thành list results.

- multiproecess: serialize (chuyển thành byte) → IPC pipe (nặng) → deserialize (chuyển byte thành python obj)”
- loky: creage mmap file -> send metdata to pipe (nhẹ) -> read data thogn qua mmap. (Zẻo cody IPC)
'''


class CoOccurrenceNumbersParallel:
    def __init__(self, prefix_partition, transaction_db, parallel=True, n_jobs=4):
        self.prefix_partition = prefix_partition
        self.transaction_db = transaction_db
        self.parallel = parallel
        self.n_jobs = n_jobs
        self.co_occurrence_numbers, self.full_co_occurrence_list = self.compute_co_occurrence_numbers()
        self.con_map = self._build_con_map()

    def compute_co_occurrence_numbers(self):
        partition_con_dict = self._build_partition_con()
        return self._merge_partition_con(partition_con_dict)

    def _build_partition_con(self):
        partition_con_dict = {}

        items = list(self.prefix_partition.prefix_partitions.items())

        # improve
        results = Parallel(n_jobs=self.n_jobs, backend="loky")(
            delayed(self._count_items_in_partition)(prefix, partition)
            for prefix, partition in items
        )
        partition_con_dict = {prefix: result for (
            prefix, _), result in zip(items, results)}

        return partition_con_dict

    @staticmethod
    def _count_items_in_partition(prefix, partition):
        START_SUBFIX_INDEX = 1
        con_i = {}
        for transaction in partition:
            if not transaction:
                continue
            prefix = transaction[0]
            con_i[prefix] = con_i.get(prefix, 0) + START_SUBFIX_INDEX
            for j in range(1, len(transaction)):
                subfix = transaction[j]
                con_i[subfix] = con_i.get(subfix, 0) + START_SUBFIX_INDEX
        return con_i

    def _merge_partition_con(self, partition_con_dict):
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

        # Sắp xếp từng prefix theo count giảm dần
        for prefix, con_list in CoN.items():
            con_list.sort(key=lambda x: x[1], reverse=True)
            CoN[prefix] = con_list

        # Gộp tất cả lại
        full_con_list = []
        for prefix, con_list in CoN.items():
            full_con_list.extend(con_list)
        full_con_list.sort(key=lambda x: x[1], reverse=True)
        return CoN, full_con_list

    def _build_con_map(self):
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
