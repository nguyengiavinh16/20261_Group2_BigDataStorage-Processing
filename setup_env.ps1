<#
  Thiet lap moi truong cho project Financial Fraud Detection tren Windows.
  Chay tu thu muc goc project (vd. "D:\My project\Financal Fraud Detection"):

    powershell -ExecutionPolicy Bypass -File .\setup_env.ps1                             # tao .venv + cai thu vien
    powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -DownloadData               # + tai PaySim vao data\raw
    powershell -ExecutionPolicy Bypass -File .\setup_env.ps1 -DownloadData -RunNotebook  # + chay lai notebook EDA

  Hoac double-click setup_env.bat.
#>
param(
    [switch]$DownloadData,
    [switch]$RunNotebook,
    [string]$Python = ""
)

$ErrorActionPreference = "Stop"
$Root = $PSScriptRoot
Set-Location -LiteralPath $Root

$CsvName       = "PS_20174392719_1491204439457_log.csv"
$ExpectedBytes = 493534783
$ExpectedSha   = "16910F90577B0D981BF8FF289714510BB89BC71BFF7D3F220F024E287E4EEA6B"
$DataUrl       = "https://storage.googleapis.com/spls/gsp774/archive.zip"
$RawDir        = Join-Path $Root "data\raw"
$CsvPath       = Join-Path $RawDir $CsvName
$Venv          = Join-Path $Root ".venv"
$VenvPy        = Join-Path $Venv "Scripts\python.exe"

# ---------- 1. Tim Python >= 3.10 ----------
Write-Host "==> [1/4] Kiem tra Python" -ForegroundColor Cyan
$PyCmd = $null; $PyArgs = @()
if ($Python) {
    $PyCmd = $Python
} elseif (Get-Command py -ErrorAction SilentlyContinue) {
    $PyCmd = "py"; $PyArgs = @("-3")
} elseif (Get-Command python -ErrorAction SilentlyContinue) {
    $PyCmd = "python"
}
if (-not $PyCmd) {
    throw "Khong tim thay Python. Cai Python 3.11+ tu https://www.python.org/downloads/ (tick 'Add python.exe to PATH') roi chay lai."
}
& $PyCmd @PyArgs -c "import sys; print(sys.version); sys.exit(0 if sys.version_info >= (3, 10) else 1)"
if ($LASTEXITCODE -ne 0) { throw "Can Python >= 3.10 (lenh dang dung: $PyCmd $PyArgs)." }

# ---------- 2. Tao virtual env + cai thu vien ----------
Write-Host "==> [2/4] Tao/cap nhat virtual env .venv" -ForegroundColor Cyan
if (-not (Test-Path -LiteralPath $VenvPy)) {
    & $PyCmd @PyArgs -m venv $Venv
    if ($LASTEXITCODE -ne 0) { throw "Tao .venv that bai." }
}
& $VenvPy -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) { throw "Nang cap pip that bai." }
& $VenvPy -m pip install -r (Join-Path $Root "requirements.txt")
if ($LASTEXITCODE -ne 0) { throw "pip install -r requirements.txt that bai." }

# ---------- 3. Du lieu ----------
Write-Host "==> [3/4] Kiem tra du lieu PaySim" -ForegroundColor Cyan
New-Item -ItemType Directory -Force -Path $RawDir | Out-Null
if ($DownloadData -and -not (Test-Path -LiteralPath $CsvPath)) {
    $Zip = Join-Path $RawDir "paysim_archive.zip"
    Write-Host "    Dang tai ~186 MB tu $DataUrl ..."
    $ProgressPreference = "SilentlyContinue"   # tat progress bar de Invoke-WebRequest tai nhanh hon
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
    Invoke-WebRequest -Uri $DataUrl -OutFile $Zip -UseBasicParsing
    Expand-Archive -LiteralPath $Zip -DestinationPath $RawDir -Force
    Remove-Item -LiteralPath $Zip
}
if (Test-Path -LiteralPath $CsvPath) {
    $size = (Get-Item -LiteralPath $CsvPath).Length
    $sha  = (Get-FileHash -LiteralPath $CsvPath -Algorithm SHA256).Hash
    if ($size -eq $ExpectedBytes -and $sha -eq $ExpectedSha) {
        Write-Host "    OK: $CsvName ($size byte, SHA-256 khop)" -ForegroundColor Green
    } else {
        Write-Warning "File khac ban da phan tich o W1: $size byte, SHA-256 $sha. Bao lai cho nhom truoc khi so sanh so lieu."
    }
} else {
    Write-Warning "Chua co data\raw\$CsvName. Tai tu link Kaggle cua nhom roi giai nen vao data\raw, hoac chay lai voi -DownloadData."
}

# ---------- 4. Chay notebook (tuy chon) ----------
if ($RunNotebook) {
    if (-not (Test-Path -LiteralPath $CsvPath)) { throw "Can co du lieu truoc khi chay notebook." }
    Write-Host "==> [4/4] Chay notebooks\w1_eda_label_evaluation.ipynb (~1-3 phut)" -ForegroundColor Cyan
    Push-Location -LiteralPath (Join-Path $Root "notebooks")
    try {
        & $VenvPy -m jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=1800 "w1_eda_label_evaluation.ipynb"
        if ($LASTEXITCODE -ne 0) { throw "Chay notebook that bai." }
    } finally {
        Pop-Location
    }
} else {
    Write-Host "==> [4/4] Bo qua chay notebook (them -RunNotebook neu muon)" -ForegroundColor Cyan
}

Write-Host ""
Write-Host "Xong. Kich hoat moi truong:  .\.venv\Scripts\Activate.ps1" -ForegroundColor Green
Write-Host "Mo notebook:                 .\.venv\Scripts\python.exe -m jupyter notebook" -ForegroundColor Green
