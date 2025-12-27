# Benchmark Report Visualization

## Mục đích
Vẽ biểu đồ so sánh hiệu suất thuật toán parallel vs sequential cho từng dataset.

## Thông số biểu đồ
- **Trục Y**: Thời gian thực thi (giây)
- **Trục X**: Giá trị Top-K
- **Mỗi dataset**: 1 biểu đồ với 2 đường (parallel + sequential)

## Cấu trúc code

```python
class ReportReader:
    """Đọc dữ liệu từ file report đọc từ bentchmark fol"""
    def __init__(self, report_path):
        pass

class Plot:
    """Tạo và quản lý biểu đồ"""
    def __init__(self, report_reader):
        pass
    
    def show(self):
        """Hiển thị biểu đồ"""
        pass
    
    def save(self, directory):
        """Lưu biểu đồ vào thư mục"""
        pass
```