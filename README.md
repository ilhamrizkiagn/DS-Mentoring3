# Design Data Warehouse using Orchestration Luigi

## Requirement
Tools yang dibutuhkan:
1. Python (miniconda)
2. PostgreSQL (pgAdmin)
3. Linux Command Line (WSL)

## Preparation
1. Create Environment
```
conda create -n [yourenv] python=3.11
```
python 3.11 digunakan agar sesuai dengan requirements.txt

2. Activate Environment
```
conda activate [yourenv]
```
3. Install Requirements
```
pip install -r requirements.txt
```
4. Run Data Source dan Data Warehouse
```
docker compose up -d
```
5. Inisialisasi dbt (dalam folder ./pipeline/scr_query)
```
dbt init
```
6. Jalankan file python
```
python elt_pipeline.py
```
7. Membuat jadwal cronjob
```
crontab -e
```
Dengan konfigurasi jadwal
```
* * * * * [./your_elt_file.sh]
```
8. Jalankan cronjob
```
sudo service cron start
```
