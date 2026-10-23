# 20261_Group2_BigDataStorage-Processing

---

# Financial Fraud Detection — W1–W2 (Thái: Data & ML Evaluation)

Đầu ra task 5.4 cho project *Real-time Financial Fraud Detection and Alerting System*: W1 (label definition, EDA, metrics plan) và W2 (feature list, feature test plan, EDA report v1).

## Cấu trúc thư mục

```
Financal Fraud Detection\
├─ README.md
├─ requirements.txt              # thư viện cho notebook EDA và tests
├─ setup_env.ps1 / setup_env.bat # tạo .venv, cài thư viện, tải + kiểm tra dữ liệu
├─ .gitignore                    # bỏ qua .venv và data\raw
├─ data\raw\                     # đặt file CSV PaySim ở đây (không commit)
├─ docs\
│  ├─ label-definition.md        # W1
│  ├─ feature-inventory.md       # W1: rà soát cột gốc, leakage/availability
│  ├─ evaluation-plan.md         # W1, cập nhật v1.1 ở W2
│  ├─ feature-list.md            # W2: định nghĩa từng feature, feature set, null policy, leakage
│  └─ feature-test-plan.md       # W2: test cases boundary / unknown category / missing / threshold
├─ notebooks\
│  ├─ w1_eda_label_evaluation.ipynb      # W1, đã chạy, có output
│  └─ w2_feature_draft_test_plan.ipynb  # W2: từng bước của task W2, đã chạy, có output
├─ src\evaluation\
│  └─ feature_reference.py       # đặc tả chạy được của feature-list.md (không phải pipeline production)
├─ tests\
│  ├─ test_feature_reference.py
│  └─ fixtures\feature_cases.json  # ca kiểm thử dùng chung cho batch/streaming ở W4
└─ reports\
   ├─ eda_summary.json                 # số liệu W1
   ├─ feature_analysis_summary.json    # số liệu W2
   ├─ eda-report-v1.md                 # W2
   ├─ figures\fig1–fig10*.png
   ├─ templates\evaluation-report-template.md
   └─ weekly\                          # W1 Thai report.docx, W2 Thai report.docx
```

## Cài đặt trên Windows

Yêu cầu: Python 3.10+ (khuyến nghị 3.11 hoặc 3.12, tick **Add python.exe to PATH** khi cài), RAM ≥ 8 GB.

**Cách 1: double-click `setup_env.bat`.** Script tạo `.venv` và cài thư viện. Muốn tải luôn dữ liệu thì mở PowerShell tại thư mục project và chạy:

```powershell
cd "D:\My project\Financal Fraud Detection"
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -DownloadData
# chạy lại notebook EDA:
powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -DownloadData -RunNotebook
```

**Cách 2: chạy từng lệnh.**

```powershell
cd "D:\My project\Financal Fraud Detection"
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1          # nếu bị chặn: Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
python -m pip install --upgrade pip
pip install -r requirements.txt
```

## Dữ liệu

- File cần có: `data\raw\PS_20174392719_1491204439457_log.csv` (493.534.783 byte).
- SHA-256 của bản đã phân tích ở W1: `16910f90577b0d981bf8ff289714510bb89bc71bff7d3f220f024e287e4eea6b`.
- Có hai cách lấy file:
  1. Tải từ link Kaggle của nhóm (https://www.kaggle.com/datasets/mtalaltariq/paysim-data), giải nén vào `data\raw\`.
  2. Chạy `setup_env.ps1 -DownloadData`: script tải bản công khai trên Google Cloud Storage.
- Kiểm tra checksum thủ công: `Get-FileHash .\data\raw\PS_20174392719_1491204439457_log.csv -Algorithm SHA256`
- Notebook W2 còn nhận file đặt tại `data\paysim dataset.csv` (cùng nội dung, đã đối chiếu SHA-256). `.gitignore` bỏ qua `data/raw/*` và `data/*.csv` nên file dữ liệu lớn không bị commit nhầm; tuy vậy vẫn nên đặt file vào `data\raw\`.

## Chạy notebook

```powershell
.\.venv\Scripts\Activate.ps1
jupyter notebook                      # mở notebooks\w1_eda_label_evaluation.ipynb (W1) hoặc notebooks\w2_feature_draft_test_plan.ipynb (W2) rồi Run All
# hoặc chạy không giao diện:
cd notebooks
jupyter nbconvert --to notebook --execute --inplace w1_eda_label_evaluation.ipynb
jupyter nbconvert --to notebook --execute --inplace w2_feature_draft_test_plan.ipynb
```

Trong VS Code, chọn interpreter `.venv\Scripts\python.exe` làm kernel.

- Notebook W1 đọc dữ liệu từ `..\data\raw\` (hoặc từ biến môi trường `PAYSIM_CSV`), sau đó ghi lại `reports\eda_summary.json` và `reports\figures\`. Cần khoảng 1–2 GB RAM.
- Notebook W2 tìm file theo thứ tự `PAYSIM_CSV` → `data\raw\PS_20174392719_1491204439457_log.csv` → `data\paysim dataset.csv`, ghi `reports\feature_analysis_summary.json` và hình `fig6`–`fig10`. Cần khoảng 3 GB RAM (phải mã hóa 9 triệu tên tài khoản) và khoảng 3–5 phút.

Đã kiểm tra chạy được với pandas 2.2 và 3.0.

## Chạy test

```powershell
python -m pytest tests/test_feature_reference.py -v
```

Bộ test kiểm tra `src/evaluation/feature_reference.py` bằng `tests/fixtures/feature_cases.json` (mô tả ở `docs/feature-test-plan.md`). Không cần dữ liệu PaySim.
