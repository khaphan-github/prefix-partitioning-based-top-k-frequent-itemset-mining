from typing import List, Dict, Set, Tuple

'''
Mục tiêu:
- Đọc dữ liệu transaction từ file text.

Example usage:
    transaction_db = TransactionDB('path/to/transactions.txt')
'''


class TransactionDB:
    def __init__(self, file_path: str):
        self.file_path: str = file_path
        self.transactions: List[List[int]] = []
        self.all_items: Set[int] = set()
        self.load_transactions(file_path=file_path)

    def load_transactions(self, file_path) -> Tuple[List[List[int]], Set[int]]:
        try:
            with open(file_path, 'r') as file:
                for line_num, line in enumerate(file, 1):
                    line = line.strip()

                    if not line:
                        continue

                    try:
                        items = list(map(int, line.split()))
                        # Paper: Without loss of generality, let x1 ≺ x2 ≺ … ≺ xd, the items are arranged in alphabetical order
                        items_sorted = sorted(items)
                        self.transactions.append(items_sorted)

                        for item in items_sorted:
                            self.all_items.add(item)

                    except ValueError as e:
                        raise ValueError(
                            f"Invalid format on line {line_num}: {line}") from e

        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {file_path}") from e

        return self.transactions, self.all_items
