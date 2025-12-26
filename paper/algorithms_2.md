# Giải thích Method: _execute_with_tidsets

## 📋 Tổng quan
Method `_execute_with_tidsets` thực hiện **Algorithm 2: ProcessSglPartition**. Đây là phần xử lý một partition riêng lẻ, tìm kiếm các frequent itemsets (tập mục xuất hiện thường xuyên) từ các giao dịch.

---

## 🎯 Input và Output

### **INPUT:**
```
partition_item      : int                    - Mục prefix (ví dụ: mục X)
promising_items     : List[int]              - Danh sách các mục tiềm năng (gồm cả prefix)
tidset_map          : Dict[int, List[int]]   - Ánh xạ: mục → danh sách ID giao dịch chứa mục đó
min_heap            : MinHeapTopK            - Heap lưu Top-K itemsets tốt nhất tìm được
rmsup               : int                    - Ngưỡng hỗ trợ tối thiểu hiện tại
partition_size      : int                    - Tổng số giao dịch trong partition
```

### **OUTPUT:**
```
min_heap            : MinHeapTopK            - Heap cập nhật với các itemsets mới tìm được
rmsup               : int                    - Ngưỡng hỗ trợ cập nhật (có thể tăng lên)
```

---

## 🔄 Chi tiết từng bước

### **PHASE 1: Khởi tạo các 2-itemsets**

#### **Bước 1.1: Khởi tạo cấu trúc dữ liệu**
```
INPUT:  promising_items (danh sách các mục)
        partition_item (mục prefix)
        tidset_map (ánh xạ mục → tid-sets)

OUTPUT: ht  = {}                # Hash table chứa các itemsets đã phát hiện
        qe  = []                # Hàng đợi ưu tiên (theo support giảm dần)
```

**Ví dụ:**
- promising_items = [1, 2, 3, 4] (mục 1 là prefix)
- tidset_map = {1: [0,1,2], 2: [1,2], 3: [0,1], 4: [2,3]}
- ht = {}  (rỗng lúc đầu)
- qe = []  (hàng đợi rỗng)

---

#### **Bước 1.2: Tạo các 2-itemsets từ prefix và các mục khác**

**Quá trình:**
```
Với mỗi mục xj trong promising_items (từ vị trí 1 trở đi):
    1. Lấy tid-set của mục prefix (tidset_xi)
    2. Lấy tid-set của mục xj (tidset_xj)
    3. Tính giao của hai tid-set
    4. Support = độ dài giao đó
    5. Nếu Support > rmsup:
        - Thêm itemset {xi, xj} vào hash table
        - Thêm vào hàng đợi ưu tiên
```

**Ví dụ chi tiết:**

**Lần 1: xj = 2**
```
INPUT:  partition_item = 1
        xj = 2
        tidset_1 = [0,1,2]
        tidset_2 = [1,2]
        rmsup = 1

TÍNH TOÁN:
  - Giao tidset: [0,1,2] ∩ [1,2] = [1,2]
  - support = 2

OUTPUT: support (2) > rmsup (1) ✓
        ht[{1,2}] = [1,2]
        qe.push((-2, {1,2}))  # Dấu (-) để thực hiện max-heap
```

**Lần 2: xj = 3**
```
INPUT:  partition_item = 1
        xj = 3
        tidset_1 = [0,1,2]
        tidset_3 = [0,1]
        rmsup = 1

TÍNH TOÁN:
  - Giao tidset: [0,1,2] ∩ [0,1] = [0,1]
  - support = 2

OUTPUT: support (2) > rmsup (1) ✓
        ht[{1,3}] = [0,1]
        qe.push((-2, {1,3}))
```

**Lần 3: xj = 4**
```
INPUT:  partition_item = 1
        xj = 4
        tidset_1 = [0,1,2]
        tidset_4 = [2,3]
        rmsup = 1

TÍNH TOÁN:
  - Giao tidset: [0,1,2] ∩ [2,3] = [2]
  - support = 1

OUTPUT: support (1) NOT > rmsup (1) ✗ → Bỏ qua
```

**Trạng thái sau Phase 1:**
```
ht = {
    {1,2}: [1,2],  # support = 2
    {1,3}: [0,1]   # support = 2
}

qe = [
    (-2, {1,2}),
    (-2, {1,3})
]

rmsup = 1 (chưa thay đổi)
```

---

### **PHASE 2: Vòng lặp chính - Mở rộng itemsets (High-Support-First)**

#### **Bước 2.1: Lấy itemset có support lớn nhất từ hàng đợi**

**Vòng lặp 1:**
```
INPUT:  qe = [(-2, {1,2}), (-2, {1,3})]

OUTPUT: neg_support_rt = -2
        itemset_rt = {1,2}
        support_rt = 2
        
Trạng thái: qe = [(-2, {1,3})]
```

---

#### **Bước 2.2: Kiểm tra điều kiện dừng**

```
Nếu support_rt <= rmsup:
    → DỪNG vòng lặp (không còn itemset nào có thể là top-k)

Trường hợp này: 2 > 1 ✓ → Tiếp tục
```

---

#### **Bước 2.3: Cập nhật min_heap (nếu itemset có >= 3 mục)**

```
Điều kiện: len(itemset_rt) >= 3

Ví dụ: itemset_rt = {1,2} → len = 2 → BỎ QUA (không cập nhật min_heap)
       itemset_rt = {1,2,3} → len = 3 → CẬP NHẬT min_heap

Nếu cập nhật:
    INPUT:  support = 2
            itemset = (1,2,3) (chuyển sang tuple sắp xếp)
    
    OUTPUT: min_heap.insert(support, itemset)
            rmsup = min_heap.min_support()  # Ngưỡng mới
```

---

#### **Bước 2.4-2.9: Mở rộng itemset với các mục còn lại**

**Bước 2.4: Tìm mục cuối cùng trong itemset**

```
INPUT:  itemset_rt = {1,2}

OUTPUT: itemset_list = [1,2]  (sắp xếp)
        last_item = 2
```

**Bước 2.5: Tìm vị trí của mục cuối cùng trong promising_items**

```
INPUT:  last_item = 2
        promising_items = [1,2,3,4]

OUTPUT: last_pos = 1  (vị trí của mục 2)
```

**Bước 2.6: Thử mở rộng với các mục sau last_pos**

Với mỗi mục y2 ở vị trí tiếp theo (last_pos+1):

**Mở rộng với y2 = 3 (vị trí 2):**

```
INPUT:  itemset_rt = {1,2}
        last_item = 2
        y2 = 3
        promising_items = [1,2,3,4]

TÍNH TOÁN:
  1. itemset_without_last = {1,2} - {2} = {1}
  2. itemset_with_y2 = {1} ∪ {3} = {1,3}
  
  3. Kiểm tra: {1,3} có trong ht không?
     Đúng! {1,3} ∈ ht ✓
  
  4. Tính giao:
     tidset_rt = ht[{1,2}] = [1,2]
     tidset_with_y2 = ht[{1,3}] = [0,1]
     
     tidset_new = [1,2] ∩ [0,1] = [1]
     support_new = 1
  
  5. Kiểm tra: support_new (1) > rmsup (1)?
     KHÔNG ✗ → Bỏ qua

OUTPUT: Không thêm itemset mới
```

**Mở rộng với y2 = 4 (vị trí 3):**

```
INPUT:  itemset_rt = {1,2}
        y2 = 4

TÍNH TOÁN:
  1. itemset_with_y2 = {1} ∪ {4} = {1,4}
  2. Kiểm tra: {1,4} có trong ht không?
     KHÔNG ✗ → Bỏ qua ngay (theo Theorem 3)

OUTPUT: Không thêm itemset mới
```

---

#### **Bước 2.1 (Vòng lặp 2): Lấy itemset tiếp theo**

```
INPUT:  qe = [(-2, {1,3})]

OUTPUT: neg_support_rt = -2
        itemset_rt = {1,3}
        support_rt = 2
        
Trạng thái: qe = []
```

**Thực hiện tương tự các bước 2.2-2.9 cho itemset {1,3}**

Cuối cùng qe trở nên rỗng → **Kết thúc vòng lặp**

---

## 📊 Kết quả cuối cùng

```
TRƯỚC KHI CHẠY:
├─ min_heap: k itemsets tốt nhất trước đó
├─ rmsup: 1

SAU KHI CHẠY _execute_with_tidsets:
├─ min_heap: k itemsets tốt nhất (có thể có thêm {1,2,3}, {1,2,4}, v.v.)
├─ rmsup: Có thể tăng lên (ví dụ: 2, 3, ... tùy kết quả)
```

---

## 🔑 Những khái niệm quan trọng

| Khái niệm | Ý nghĩa |
|-----------|---------|
| **tidset** | Danh sách các ID giao dịch chứa mục đó |
| **support** | Số lần xuất hiện của một itemset |
| **rmsup** | Running Minimum Support - ngưỡng hỗ trợ tối thiểu hiện tại |
| **min_heap** | Heap lưu Top-K itemsets có support cao nhất |
| **high-support-first** | Ưu tiên xử lý itemsets có support cao trước (hiệu quả hơn) |
| **Theorem 3** | Nếu X∪{y} không trong ht, thì support(X∪{y}) ≤ rmsup |

---

## 💡 Tóm tắt

1. **Phase 1**: Tạo tất cả 2-itemsets từ prefix + các mục khác
2. **Phase 2**: 
   - Lấy itemset có support cao nhất
   - Cập nhật Top-K nếu itemset có 3+ mục
   - Mở rộng itemset bằng cách thêm mục mới
   - Lặp lại cho đến hết hàng đợi

**Mục tiêu**: Tìm ra các itemsets có support cao, là ứng cử viên cho Top-K itemsets.
