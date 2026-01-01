import time
import random


def compute_square(number):
    """Hàm mô phỏng xử lý một tác vụ tính toán."""
    time.sleep(random.uniform(0.1, 0.4))  # Giả lập tác vụ nặng
    return number * number


def run_single_core(data):
    """
    Chạy toàn bộ công việc trên 1 core (tuần tự).
    Giống logic của worker + aggregator, nhưng gộp lại.
    """
    total_sum = 0
    for idx, number in enumerate(data, start=1):
        result = compute_square(number)
        total_sum += result
        print(f"[Main] Xử lý {number}^2 = {result} ({idx}/{len(data)})")
    print(f"\n[Main] Tổng hợp hoàn tất! Tổng = {total_sum}")


if __name__ == "__main__":
    data = list(range(1, 21))  # 1..20
    start_time = time.perf_counter()
    run_single_core(data)
    end_time = time.perf_counter()
    print(f"\n[Main] Thời gian chạy tuần tự: {end_time - start_time:.4f}s")
