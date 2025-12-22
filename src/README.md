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
| `python3 main_with_json_config.py config_parallel.json`   | Xử lý song song |
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
| `input_dataset_path` | Đường dẫn file dữ liệu input               |
| `output_report`      | Thư mục lưu kết quả (dùng để vẽ biểu đồ)   |
| `save_metrics`       | Bật/tắt lưu metrics                        |

## Quy trình xử lý

1. Chạy `main_with_json_config.py` với file config tương ứng
2. Dữ liệu từ `/data` → Thuật toán xử lý → Kết quả lưu vào `/benchmark`
3. Sử dụng notebook để vẽ biểu đồ từ dữ liệu trong `/benchmark`
