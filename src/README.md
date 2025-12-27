# Parallel Processing Report

## Thiết lập

```bash
cd src
pip install -r requirments.txt
```

## Chạy chương trình

| Lệnh                                                      | Mô tả           |
| --------------------------------------------------------- | --------------- |
| `python3 main_with_json_config.py config_sequential.json` | Xử lý tuần tự   |
| `python3 main_with_json_config.py config_multiprocessing.json`   | Xử lý song song |
| `python3 main.py`                                         | Chạy mặc định   |

## Kiểm thử

```bash
python -m pytest ./tests/ -v
```

## Cấu hình JSON

Chỉnh sửa file `config_*.json`:

| Tham số              | Mô tả                                      |
| -------------------- | ------------------------------------------ |
| `top_k`              | Số frequent itemset cần lấy (mặc định: 20) |
| `parallel`           | Bật/tắt xử lý song song (mặc định: false)  |
| `num_workers`        | Số luồng xử lý song song (chỉ khi parallel=true) |
| `input_dataset_path` | Đường dẫn file dữ liệu input               |
| `output_report`      | Thư mục lưu kết quả (dùng để vẽ biểu đồ)   |
| `save_metrics`       | Bật/tắt lưu metrics                        |

### Song song hóa cấp độ Phân vùng (Partition-Level Parallelism)

Khi `parallel: true`, thuật toán sử dụng Thread Pool để xử lý các phân vùng song song:

- Mỗi phân vùng $P_i$ (nếu không bị cắt tỉa) được xử lý bởi một luồng riêng biệt
- Sử dụng `ThreadPoolExecutor` với số lượng worker được cấu hình bởi `num_workers`
- Mỗi worker có bản sao cục bộ của Min-Heap (MH) để tránh xung đột
- Kết quả từ tất cả các worker được gộp lại sau khi hoàn thành

**Lợi ích:**
- Tăng tốc độ xử lý trên hệ thống đa nhân
- Tận dụng tối đa tài nguyên CPU
- Giảm thời gian thực thi cho các tập dữ liệu lớn

## Quy trình xử lý

1. Chạy `main_with_json_config.py` với file config tương ứng
2. Dữ liệu từ `/data` → Thuật toán xử lý → Kết quả lưu vào `/benchmark`
3. Sử dụng notebook để vẽ biểu đồ từ dữ liệu trong `/benchmark`
