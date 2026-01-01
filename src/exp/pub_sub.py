from multiprocessing import Process, JoinableQueue, cpu_count
import time
import os
import random


def worker_task(worker_id, data_chunk, queue):
    """
    Worker xử lý dữ liệu, đẩy kết quả vào queue.
    """
    for number in data_chunk:
        result = number * number
        # Giả lập độ trễ xử lý
        time.sleep(random.uniform(0.1, 0.4))
        queue.put((worker_id, number, result))
    print(f"[Worker {worker_id}] Hoàn tất {len(data_chunk)} tác vụ.")


def aggregator_task(queue, total_tasks):
    """
    Aggregator lấy kết quả từ queue, tổng hợp.
    """
    processed = 0
    total_sum = 0

    while processed < total_tasks:
        worker_id, number, result = queue.get()
        print(f"[Aggregator] Nhận {number}^2 = {result} từ Worker {worker_id}")
        total_sum += result
        processed += 1
        queue.task_done()  # Báo hiệu task hoàn tất

    print(f"\n[Aggregator] Tổng hợp hoàn tất! Tổng = {total_sum}")


if __name__ == "__main__":
    cpu_total = cpu_count()
    num_workers = cpu_total - 1  # dùng 7 core tính toán, 1 core aggregator
    queue = JoinableQueue()

    # Dữ liệu mẫu
    data = list(range(1, 21))  # 1..20
    chunk_size = max(1, len(data) // num_workers)
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    total_tasks = len(data)

    # Tạo workers
    workers = [Process(target=worker_task, args=(i, chunks[i], queue))
               for i in range(len(chunks))]

    # Tạo aggregator
    aggregator = Process(target=aggregator_task, args=(queue, total_tasks))

    # Khởi động
    aggregator.start()
    for w in workers:
        w.start()

    # Đợi tất cả task được xử lý (báo hiệu bởi queue.task_done)
    queue.join()

    # Kết thúc
    for w in workers:
        w.join()
    aggregator.join()

    print("\n[Main] Hoàn tất toàn bộ tiến trình.")
