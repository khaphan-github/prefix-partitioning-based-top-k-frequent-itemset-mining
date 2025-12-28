# Check list
# From repo root
# Use 4 workers for parallel runs
./run_all.sh

# Top K
1000
500
250
100
50
10

# Dataset
- [x] Pumsb
- [x] Chain store
- [x] Accidents.txt
- [x] OnlineRetails
- [x] Susy

# TODO Next:
- [ ] Run lai report cho khanh then thong itn io cost.
      + Du lieu can so sanh:
        - Thuat toan truoc cai tien & sau cai kien cua tat ca cac dataset
        - Print phan IO biyte truoc khi cai tien va sua khi cairhtien
- [ ] Fileword: Ung voi tung buoc trong bai bao thi in ra input & output cua tung step.

- [ ] Benchmark: Can bieu do the hien topk & time voi tuan tu xong xong, them thong tin in của IO memory giong bai bao de do duocj hieu nang so voi lúc trước cải tiến

- [ ] & lúc sau cải tiến ứng ơi 2 loaid datta set la chainstore & pumsb.

- [ ] Side Lem bo cung giong silde, nhan manh cải tiến & cơ chế lưu trử của tác,gian
# Hardware info.
H/W path           Device          Class          Description
=============================================================
                                   system         System Product Name (SKU)
/0/c                               memory         32GiB System Memory
/0/23                              processor      12th Gen Intel(R) Core(TM) i7-12700
/0/100                             bridge         12th Gen Core Processor Host Bridge/DRAM Registers


# Run All
Below are consistent command blocks for each dataset. Each has a Sequential block and a Parallel block (with `--workers 4`). Top-k values: 1000, 500, 250, 100, 50, 10.

## OnlineRetailZZ.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/OnlineRetailZZ.txt --output ./benchmark/report_27_12/OnlineRetailZZ/multiprocessing/ --parallel --workers 4
```

## SUSY.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/SUSY.txt --output ./benchmark/report_27_12/SUSY/multiprocessing/ --parallel --workers 4
```

## accidents.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/accidents.txt --output ./benchmark/report_27_12/accidents/multiprocessing/ --parallel --workers 4
```

## accidents_spmf.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/accidents_spmf.txt --output ./benchmark/report_27_12/accidents_spmf/multiprocessing/ --parallel --workers 4
```

## chainstoreFIM.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/chainstoreFIM.txt --output ./benchmark/report_27_12/chainstoreFIM/multiprocessing/ --parallel --workers 4
```

## connect.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/connect.txt --output ./benchmark/report_27_12/connect/multiprocessing/ --parallel --workers 4
```

## pumsb.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/pumsb.txt --output ./benchmark/report_27_12/pumsb/multiprocessing/ --parallel --workers 4
```

## sample.txt
Sequential
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
python3 main_with_args.py --top-k 500 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
python3 main_with_args.py --top-k 250 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
python3 main_with_args.py --top-k 100 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
python3 main_with_args.py --top-k 50 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
python3 main_with_args.py --top-k 10 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/sequential/
```
Parallel (workers=4)
```bash
python3 main_with_args.py --top-k 1000 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 500 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 250 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 100 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 50 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
python3 main_with_args.py --top-k 10 --input ./data/data_set/sample.txt --output ./benchmark/report_27_12/sample/multiprocessing/ --parallel --workers 4
```