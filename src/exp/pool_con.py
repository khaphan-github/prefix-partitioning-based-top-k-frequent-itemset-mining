from concurrent.futures import ProcessPoolExecutor
import os
import time

'''
GIL
- GIL (Global Interpreter Lock) là một khóa toàn cục trong CPython 
- một thời điểm, chỉ có duy nhất một luồng (thread) được thực thi mã bytecode Python trong một tiến trình.
(GIL ngăn không cho nhiều thread chạy đồng thời trên nhiều CPU trong cùng một process.)

not: import threading 
ok: concurrent
'''


def work(n):
    total = 0
    for i in range(10_000_000):
        total += i * n
    return total


if __name__ == "__main__":
    cpu_total = os.cpu_count()
    print(f"Số CPU khả dụng: {cpu_total}")

    start_seq = time.time()
    results_seq = [work(n) for n in range(10)]
    end_seq = time.time()
    print(f"Thời gian chạy tuần tự: {end_seq - start_seq:.4f} giây")

    start_par = time.time()
    with ProcessPoolExecutor(max_workers=cpu_total) as executor:
        results_par = list(executor.map(work, range(10)))
    end_par = time.time()
    print(f"Thời gian chạy song song: {end_par - start_par:.4f} giây")

    # So sánh
    speedup = (end_seq - start_seq) / (end_par - start_par)
    print(f"Tốc độ nhanh hơn khoảng: {speedup:.2f} lần")
