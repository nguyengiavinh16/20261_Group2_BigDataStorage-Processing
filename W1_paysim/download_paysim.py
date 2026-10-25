#!/usr/bin/env python3
"""Download the original PaySim dataset (Kaggle: ealaxi/paysim1) into data/raw/.

Requires: pip install kagglehub
Kaggle credentials: set KAGGLE_USERNAME and KAGGLE_KEY, or place kaggle.json in ~/.kaggle/
"""
import glob
import os
import shutil

import kagglehub

HERE = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(HERE, "data", "raw")

cache_path = kagglehub.dataset_download("ealaxi/paysim1")
print("Path to dataset files:", cache_path)

os.makedirs(RAW_DIR, exist_ok=True)
for src in glob.glob(os.path.join(cache_path, "*")):
    dst = os.path.join(RAW_DIR, os.path.basename(src))
    shutil.copy2(src, dst)
    print("Copied to", dst)
