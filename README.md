# JNE Data Pipeline v2

Pipeline data untuk mengambil data pengiriman JNE dari Oracle, menyimpannya
sebagai Parquet di MinIO (data lake "bronze"), menjalankan pengecekan kualitas
data (governance), lalu memuat hasilnya ke ClickHouse untuk dashboard Tableau.

Dokumen ini menjelaskan:

1. [Apa isi repository ini](#1-apa-isi-repository-ini)
2. [Alur kerja pipeline](#2-alur-kerja-pipeline)
3. [Struktur folder](#3-struktur-folder)
4. [Cara menjalankan pipeline](#4-cara-menjalankan-pipeline)
5. [Cara mengubah fitur](#5-cara-mengubah-fitur)
6. [Membaca hasil governance](#6-membaca-hasil-governance)
7. [Testing](#7-testing)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Apa isi repository ini

Pipeline ini sengaja dibuat sederhana dan fokus pada empat hal:

| Tahap | Fungsi | Kode |
|---|---|---|
| **Extract** | Mengambil tabel Oracle apa adanya (tanpa join/dedupe) ke Parquet | `extractor/` |
| **Transform** | Memperkaya tabel CNOTE dan membuat tabel relasi dokumen → CNOTE | `transform/` |
| **Governance** | Menjalankan ±493 index pengecekan kualitas data | `governance/` |
| **Load** | Memuat data bronze + hasil governance ke ClickHouse | `loader/` |

Semua tahap diorkestrasi oleh Airflow lewat DAG `jne_data_pipeline`.

Prinsip penting:

- **Bronze = salinan mentah.** Saat extract tidak ada join, dedupe, atau pivot.
  Setiap tabel Oracle disimpan sendiri-sendiri.
- **MinIO adalah sumber data yang permanen.** Folder `data/` lokal hanya
  tempat sementara.
- **Semua tahap dibaca lewat `run_manifest.json`.** Manifest mencatat tabel apa
  saja yang ada di satu run, sehingga tahap berikutnya tahu file mana yang harus
  dibaca.

---

## 2. Alur kerja pipeline

```text
                ┌──────────────┐
  Oracle (JNE)  │ extract_oracle│  extractor.bronze
  ─────────────▶│              │──────────────▶ MinIO: bronze/jne/.../run_id=<id>/
                └──────┬───────┘                 ├── CMS_CNOTE/part-*.parquet
                       │                         ├── CMS_MANIFEST/part-*.parquet
                       ▼                         ├── ...
                ┌──────────────┐                 └── run_manifest.json
                │transform_data│  transform.transform_data
                │              │──────────────▶ derived/cms_cnote_transformed/
                └──────┬───────┘                derived/document_cnote_links/
                       ▼
                ┌──────────────┐
                │run_governance│  governance.runner
                │              │──────────────▶ governance_results.csv
                └──────┬───────┘                governance_result_cnotes.csv
                       ▼                        governance_rule_summary.csv
                ┌────────────────────────┐
                │load_data_mart_clickhouse│ loader.mart_load_clickhouse
                │                        │──▶ ClickHouse: bronze.*, governance.*, mart.*
                └────────────────────────┘          │
                                                     ▼
                                                  Tableau
```

### 2.1 Extract (Oracle → Parquet)

1. **Menentukan CNOTE yang diambil.** Pipeline memilih CNOTE dari
   `CMS_CNOTE` berdasarkan `CNOTE_DATE` di dalam window waktu
   (`extraction.window`). Jika `cnote_limit` diisi, hanya N CNOTE pertama yang
   diambil (saat ini `100000`).
2. **Membuat "scope table" di Oracle.** Daftar CNOTE tersebut disimpan sebagai
   tabel sementara di schema `HOA` (prefix `BRONZE_SCOPE_`). Dari daftar ini
   pipeline menurunkan daftar dokumen lain: manifest, bag, SMU, runsheet, DO,
   dan seterusnya.
3. **Mengambil tabel per tahap (stage):**

   | Stage | Contoh tabel | Cara filter |
   |---|---|---|
   | `anchor` | `CMS_CNOTE` | Window tanggal (`CNOTE_DATE`) |
   | `cnote` | `CMS_DRCNOTE`, `CMS_MFCNOTE`, `CMS_APICUST`, `CMS_CNOTE_POD` | CNOTE yang masuk scope |
   | `bag_manifest` | `CMS_MANIFEST`, `CMS_MFBAG`, `CMS_DMBAG`, `CMS_MMBAG`, `CMS_DSMU`, `CMS_MSMU` | Dokumen yang terhubung ke scope |
   | `runsheet_do` | `CMS_MRSHEET`, `CMS_MSJ`, `CMS_DSJ`, `CMS_MHICNOTE`, `CMS_MHOCNOTE` | Dokumen yang terhubung ke scope |
   | `reference` | `CMS_DROURATE`, `ORA_BRANCH`, `ORA_ZONE`, `ORA_USER` | Diambil penuh, lalu **dipakai ulang** di run berikutnya |

4. **Menulis Parquet** ke folder lokal, upload ke MinIO, lalu menulis
   `_SUCCESS` per tabel dan `run_manifest.json` untuk satu run.
5. Scope table di Oracle dihapus setelah selesai (kecuali `keep_scope: true`).

Lokasi output di MinIO:

```text
s3://jne-bronze/bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id>/
```

### 2.2 Transform

`transform/transform_data.py` membaca Parquet bronze (memakai DuckDB) dan
menghasilkan:

- `derived/cms_cnote_transformed/`: CNOTE yang sudah diperkaya (misalnya
  kategori pengiriman dan SLA/ETD yang dinormalisasi). Di ClickHouse, data ini
  **menggantikan** `bronze.cms_cnote` mentah.
- `derived/document_cnote_links/`: tabel relasi dokumen non-CNOTE (bag,
  manifest, runsheet, dll.) ke CNOTE. Dipakai governance untuk mengetahui CNOTE
  mana yang terdampak oleh suatu dokumen. Logika relasinya ada di
  `transform/document_links.py`.

Hasil transform dicatat di bagian `derived` pada `run_manifest.json`.

### 2.3 Governance

`governance/runner.py` membaca manifest, memuat kolom yang dibutuhkan saja,
lalu menjalankan setiap index di `governance/catalog.py`. Setiap index
menghasilkan status:

| Status | Arti |
|---|---|
| `PASS` | Rule berjalan, tidak ada baris yang gagal |
| `FAIL` | Rule berjalan, ada baris yang gagal |
| `NO_ROWS` | Tabel ada tetapi tidak ada data yang dicek |
| `SKIPPED` | Tabel/kolom yang dibutuhkan tidak ada di manifest |
| `ERROR` | Rule gagal dijalankan (misalnya kolom salah atau rule tidak valid) |

Secara default proses governance **gagal (exit non-zero) jika ada `ERROR`**.

### 2.4 Load ke ClickHouse

`loader/mart_load_clickhouse.py` memuat:

- Database `bronze`: semua tabel bronze dari run terakhir (dengan
  `cms_cnote` diganti versi transformed). Tabel reference hanya dimuat jika
  belum ada.
- Database `governance`: `governance_results`, `governance_result_cnotes`,
  `governance_rule_summary`. Tabel ini dibuat ulang setiap load.
- Database `mart`: tabel gabungan `unified_shipments` dari
  `loader/sql/unified_shipments.sql`.

---

## 3. Struktur folder

```text
.
├── airflow/dags/jne_data_pipeline_dag.py  # Definisi DAG Airflow
├── config/
│   ├── config.yaml                        # Oracle, window, scoping, MinIO, output
│   ├── mart_clickhouse.yaml               # Koneksi & pengaturan mart ClickHouse
│   └── pii_exclude.yaml                   # Kolom PII yang dikecualikan
├── extractor/bronze.py                    # Extract Oracle → Parquet → MinIO
├── transform/
│   ├── transform_data.py                  # Enrichment CNOTE
│   └── document_links.py                  # Relasi dokumen → CNOTE
├── governance/
│   ├── catalog.py                         # Daftar semua index (rule) yang dicek
│   ├── rules.py                           # Implementasi tiap jenis rule
│   ├── runner.py                          # Menjalankan catalog terhadap satu run
│   └── output.py                          # Penulis file hasil (CSV/Parquet)
├── loader/
│   ├── mart_load_clickhouse.py            # Loader ClickHouse
│   └── sql/unified_shipments.sql          # SQL tabel mart gabungan
├── pipeline_context.py                    # Helper Airflow (prefix run, window)
├── tests/                                 # Test regresi
├── docker-compose.yml                     # MinIO, ClickHouse, Airflow
└── Dockerfile.airflow
```

---

## 4. Cara menjalankan pipeline

### 4.1 Persiapan

1. Salin file environment dan isi nilainya:

   ```bash
   cp .env.example .env
   ```

   Variabel yang wajib diisi:

   | Variabel | Keterangan |
   |---|---|
   | `ORACLE_HOST`, `ORACLE_PORT` | Alamat database Oracle |
   | `ORACLE_SID` atau `ORACLE_SERVICE` | Salah satu (SID diprioritaskan) |
   | `ORACLE_USER`, `ORACLE_PASSWORD` | Kredensial Oracle (user perlu akses ke schema `HOA` untuk scope table) |
   | `ORACLE_SCHEMA` | Schema sumber, default `JNE` |
   | `AIRFLOW_ADMIN_USER`, `AIRFLOW_ADMIN_PASSWORD` | Login Airflow |

   `.env` tidak boleh di-commit.

2. Jalankan semua service:

   ```bash
   docker compose up -d --build
   ```

### 4.2 Alamat service

| Service | Alamat dari laptop | Alamat dari dalam container | Login default |
|---|---|---|---|
| Airflow UI | `http://localhost:<AIRFLOW_PORT>` (default `8080`, contoh `.env` memakai `8081`) | - | dari `.env` |
| MinIO Console | `http://localhost:9001` | - | `minioadmin` / `minioadmin123` |
| MinIO API | `localhost:9000` | `minio:9000` | sama |
| ClickHouse HTTP | `localhost:8123` | `mart-clickhouse:8123` | `default` / `jne_mart` |
| ClickHouse native | `localhost:9002` | `mart-clickhouse:9000` | sama |
| CH-UI (opsional) | `http://localhost:3488` | - | aktifkan dengan `docker compose --profile ui up -d` |

### 4.3 Menjalankan lewat Airflow (cara utama)

1. Buka Airflow UI, cari DAG **`jne_data_pipeline`**.
2. Klik **Trigger DAG**. DAG tidak punya jadwal otomatis (`schedule_interval=None`).
3. Urutan task:

   ```text
   extract_oracle → transform_data → run_governance → load_data_mart_clickhouse
   ```

Opsi tambahan lewat **Trigger DAG w/ config** (`dag_run.conf`):

| Config | Fungsi |
|---|---|
| `{"keep_scope": true}` | Scope table di Oracle tidak dihapus (untuk inspeksi manual) |
| `{"load_clickhouse": false}` | Lewati load ke ClickHouse untuk run ini |

Window waktu diambil dari `config/config.yaml`, **bukan** dari `dag_run.conf`.
Untuk mengganti periode, ubah config lalu trigger ulang DAG.

### 4.4 Menjalankan manual per tahap

Berguna untuk debugging. Jalankan dari root repository (atau di dalam container
Airflow di `/opt/airflow/project`).

**Extract:**

```bash
python -m extractor.bronze --config config/config.yaml --run-id local_test
```

**Transform** (dari MinIO):

```bash
BRONZE_RUN_PREFIX=bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id> \
python -m transform.transform_data --source minio --config config/config.yaml
```

atau dari folder lokal:

```bash
python -m transform.transform_data --source local --bronze-run-path data/bronze/.../run_id=<run_id>
```

**Governance** (dari MinIO):

```bash
BRONZE_RUN_PREFIX=bronze/jne/window_start=YYYY-MM-DD/window_end=YYYY-MM-DD/extract_date=YYYY-MM-DD/run_id=<run_id> \
python -m governance.runner --source minio --config config/config.yaml --output-dir governance/outputs/<run_id>
```

atau dari folder lokal:

```bash
python -m governance.runner --source local --bronze-run-path data/bronze/.../run_id=<run_id> --output-dir governance/outputs/<run_id>
```

Opsi governance:

| Flag | Fungsi |
|---|---|
| `--no-strict` | Tidak gagal walaupun ada `ERROR` (hanya untuk inspeksi) |
| `--fail-on-skipped` | Gagal jika ada rule aktif yang `SKIPPED` |
| `--source synthetic` | Pakai data contoh bawaan, tanpa Oracle/MinIO |

**Load ke ClickHouse:**

```bash
python -m loader.mart_load_clickhouse --config config/mart_clickhouse.yaml
```

**Helper Airflow** (melihat prefix run dan window yang akan dipakai):

```bash
python -m pipeline_context bronze-prefix --config config/config.yaml --run-id local_test --extract-date 2026-06-10
python -m pipeline_context window --config config/config.yaml start
python -m pipeline_context window --config config/config.yaml end
```

---

## 5. Cara mengubah fitur

### 5.1 Mengubah periode waktu (window)

Di `config/config.yaml`:

```yaml
extraction:
  window:
    mode: "explicit"
    start_date: "2026-05-01"   # inklusif
    end_date: "2026-06-01"     # eksklusif
```

Atau per bulan:

```yaml
  window:
    mode: "relative"
    anchor_month: "2026-05"
    num_months: 1
```

### 5.2 Mengambil semua CNOTE (tanpa batas 100.000)

Kosongkan atau hapus `cnote_limit`:

```yaml
extraction:
  cnote_limit:        # kosong = semua CNOTE di dalam window
```

Hal yang perlu diperhatikan:

- Volume data naik drastis, termasuk semua tabel turunan. Mulai dengan window
  pendek (1 hari atau 1 minggu) lalu cek jumlah baris di `run_manifest.json`.
- Governance memuat data ke memori (pandas). Pantau RAM worker Airflow saat
  menjalankan window besar.
- Koordinasikan dengan pemilik database Oracle karena query scope menjadi
  lebih berat.

### 5.3 Memilih tabel yang di-extract

```yaml
extraction:
  tables: ["CMS_CNOTE"]   # hanya CMS_CNOTE (smoke test)
  # tables: []            # kosong = semua tabel
```

Index governance yang membutuhkan tabel yang tidak di-extract akan berstatus
`SKIPPED`.

### 5.4 Menonaktifkan atau menghapus index governance

Semua index ada di `governance/catalog.py` dalam list `CATALOG`. Saat ini ada
493 index dan semuanya aktif.

**Cara yang disarankan: nonaktifkan.** Tambahkan `'enabled': False` pada entry:

```python
{
    'index_code': 'VALD1B6',
    'enabled': False,
    'element': 'Validity',
    ...
},
```

Index yang dinonaktifkan tidak dijalankan, tidak menghasilkan baris hasil, dan
tabelnya tidak dimuat khusus untuk index tersebut. Jumlahnya tercatat di log
sebagai `Disabled entries`. Cara ini mudah dikembalikan.

**Alternatif: hapus entry.** Hapus seluruh blok `{...}`. Periksa dulu
`tests/test_governance_runner.py`, karena beberapa kode index (misalnya
`COMP1B1`, `ACCU4B15`, `ACCU3B13B`, `COMP1V19`) dicek langsung di test. Jika
dihapus, test tersebut juga harus diubah.

**Hanya menyembunyikan di dashboard.** Filter kolom `index_code` di Tableau.
Rule tetap berjalan, pipeline tidak perlu diubah.

Tips: cari dengan `index_code` (misalnya `Ctrl+F` → `'COMP1B1'`) karena file
catalog cukup panjang.

### 5.5 Mengubah atau menambah index governance

Struktur satu entry:

```python
{
    'index_code': 'VALD1B6',             # kode unik index
    'element': 'Validity',               # Completeness / Validity / Consistency / Uniqueness / Timeliness / Accuracy
    'indicator': 'Service Code',
    'rule_family': 'validity_regex',     # jenis pengecekan (fungsi di rules.py)
    'table': 'CMS_CNOTE',                # tabel utama yang dicek
    'params': {                          # parameter khusus rule_family
        'column': 'CNOTE_SERVICES_CODE',
        'pattern': '^[A-Z0-9]{2,5}$',
        'cnote_column': 'CNOTE_NO',      # kolom yang berisi CNOTE / nomor dokumen
    },
    'main_impact': 'TBD',                # metadata untuk dashboard
    'impact_details': 'TBD',
    'issue_description': 'Invalid Service Code Format',
    'description': 'CMS_CNOTE.CNOTE_SERVICES_CODE must look like a service code',
},
```

- **Mengubah rule:** ubah nilai di `params` (kolom, pattern, tabel referensi,
  dan seterusnya).
- **Menambah rule dengan jenis yang sudah ada:** salin entry lain dengan
  `rule_family` yang sama, lalu ganti `index_code`, `table`, dan `params`.
  `index_code` harus unik.
- **Menambah jenis rule baru:** tulis fungsi baru di `governance/rules.py` yang
  mengembalikan `RuleOutcome`, daftarkan di `RULE_FUNCTIONS`, lalu gunakan
  namanya sebagai `rule_family`.

Jenis rule (`rule_family`) yang paling sering dipakai:

| rule_family | Fungsi |
|---|---|
| `completeness` | Kolom tidak boleh null/kosong |
| `conditional_completeness` | Kolom wajib terisi jika kondisi tertentu terpenuhi |
| `uniqueness` | Kombinasi kolom harus unik |
| `validity_regex` | Nilai harus cocok dengan pola regex |
| `validity_datetime` / `validity_integer` | Nilai harus berupa tanggal / angka valid |
| `reference_format` / `value_in_reference` | Nilai harus ada di tabel referensi (misalnya `CMS_DROURATE`) |
| `pair_consistency` | Dua kolom dari tabel berbeda harus sama |
| `bridged_pair_consistency` | Sama seperti di atas, tetapi lewat tabel penghubung |
| `aggregate_sum_consistency` / `aggregate_count_consistency` | Total di master harus sama dengan jumlah di detail |
| `timeliness` / `bridged_timeliness` | Urutan atau selisih waktu antar proses |

Setelah mengubah catalog, jalankan pengecekan cepat (lihat [Testing](#7-testing)).

### 5.6 Mengubah pemetaan region

Kolom `origin_region` dan `destination_region` di hasil governance dihitung
dari 3 huruf pertama kode origin/destination (misalnya `CGK` → `Jakarta`).
Pemetaannya ada di `BRANCH_REGION_BY_CODE` di `governance/runner.py`. Kode yang
tidak terdaftar menjadi `Other`.

### 5.7 Filter berdasarkan area, layanan, dan sejenisnya

- **Untuk laporan:** hasil governance sudah memiliki kolom `cnote_origin`,
  `cnote_destination`, `origin_region`, `destination_region`,
  `cnote_service_code`, dan `shipment_type`. Filter langsung di Tableau atau
  ClickHouse, tanpa mengubah pipeline.
- **Untuk mengurangi data yang di-extract:** belum ada pengaturan di config.
  Perlu perubahan kode di `extractor/bronze.py` pada dua tempat:
  1. Query scope CNOTE di `materialize_scope_tables` (tempat filter tanggal
     CNOTE dibuat).
  2. Predicate tabel anchor `CMS_CNOTE` di `_build_sql`, karena tanpa
     `cnote_limit` tabel ini difilter dengan tanggal saja.

  Perhatikan bahwa satu manifest atau bag bisa berisi CNOTE dari beberapa
  area, sehingga rule hitungan dan berat pada dokumen tersebut bisa terlihat
  tidak cocok jika data difilter per area.

### 5.8 Menambah relasi dokumen → CNOTE

Relasi dokumen ke CNOTE dibuat di `transform/document_links.py`. Hanya tambahkan
relasi yang sudah **terkonfirmasi** dari sumber data. Contoh relasi yang sudah
ada:

- `CMS_MFBAG.MFBAG_NO → CMS_MFCNOTE.MFCNOTE_BAG_NO → CNOTE`
- `CMS_MANIFEST.MANIFEST_NO → CMS_MFCNOTE.MFCNOTE_MAN_NO → CNOTE`
- `CMS_MRSHEET.MRSHEET_NO → CMS_DRSHEET.DRSHEET_NO → CNOTE`

Daftar lengkap ada di `AGENTS.md`.

### 5.9 Mengecualikan kolom PII

1. Isi kolom per tabel di `config/pii_exclude.yaml`:

   ```yaml
   CMS_CNOTE: ["NAMA_KOLOM_1", "NAMA_KOLOM_2"]   # ganti dengan nama kolom asli
   ```

2. Ubah mode kolom di `config/config.yaml`:

   ```yaml
   columns:
     mode: "exclude_pii"
   ```

Pastikan kolom yang dikecualikan tidak dibutuhkan oleh index governance. Jika
dibutuhkan, index tersebut akan berstatus `SKIPPED`.

### 5.10 Tuning performa extract

Di `config/config.yaml`:

| Pengaturan | Fungsi |
|---|---|
| `oracle.fetch_arraysize` | Jumlah baris per batch fetch |
| `oracle.prefetch_rows` | Prefetch Oracle client, sebaiknya `fetch_arraysize + 1` |
| `scoping.ctas_parallel_degree` | Hint `PARALLEL(n)` untuk pembuatan scope table |
| `scoping.scope_workers` | Jumlah job scope yang berjalan bersamaan |
| `scoping.date_guardrail_lookahead_days` | Berapa hari setelah window tabel turunan masih diambil (default 30) |
| `output.rows_per_file` | Jumlah baris per file Parquet |

### 5.11 Pengaturan mart ClickHouse

Di `config/mart_clickhouse.yaml`:

- `governance.enabled: false`: tidak memuat hasil governance.
- `unified_mart.enabled: false`: tidak membuat `mart.unified_shipments`.
- `mart.reuse_existing_stages: ["reference"]`: tabel reference tidak dimuat
  ulang jika sudah ada di ClickHouse.

---

## 6. Membaca hasil governance

Tiga output utama (CSV dan Parquet di `governance/outputs/<run_id>/`, juga di
ClickHouse database `governance`):

| Tabel | Isi |
|---|---|
| `governance_results` | Satu baris per dokumen yang dicek per index. Kolom penting: `result_id`, `index_code`, `status`, `document_type`, `document_id`, `cnote_no` |
| `governance_result_cnotes` | Relasi `result_id` → `cnote_no` (nol, satu, atau banyak CNOTE) |
| `governance_rule_summary` | Satu baris per index: status dan alasan jika `SKIPPED`/`ERROR` |

Aturan untuk dashboard:

- **Tampilan per dokumen:** gunakan `governance_results.document_id`.
- **Tampilan per CNOTE:** join `governance_results` ke
  `governance_result_cnotes` pada `result_id`, lalu gunakan
  `governance_result_cnotes.cnote_no`.
- **Jangan** mengelompokkan tabel non-CNOTE hanya dengan
  `governance_results.cnote_no`. Kolom ini sengaja dikosongkan jika dokumen
  terhubung ke nol atau banyak CNOTE.

---

## 7. Testing

Install dependency lalu jalankan test:

```bash
pip install -r requirements.txt pandas pytest
python -m pytest tests
```

Pengecekan cepat tanpa Oracle/MinIO:

```bash
# Cek syntax semua modul
python -m compileall extractor loader governance transform airflow/dags tests pipeline_context.py

# Jalankan governance dengan data sintetis
python -m governance.runner --source synthetic --no-strict --output-dir /tmp/jne-governance-smoke
```

Jalankan keduanya setiap kali mengubah `governance/catalog.py` atau
`governance/rules.py`.

---

## 8. Troubleshooting

| Gejala | Kemungkinan penyebab |
|---|---|
| Banyak index `SKIPPED` | `extraction.tables` hanya berisi sebagian tabel, atau kolom dikecualikan lewat PII |
| Governance gagal dengan `rule implementation error(s)` | Ada index berstatus `ERROR`. Cek `governance_rule_summary.csv` untuk pesan error. Gunakan `--no-strict` hanya untuk inspeksi |
| Task governance mati / kehabisan memori | Window terlalu besar atau `cnote_limit` dikosongkan. Perkecil window |
| Extract lambat di tahap scope | Naikkan `ctas_parallel_degree` / `scope_workers` (koordinasi dengan DBA) |
| Scope table tertinggal di schema `HOA` | Run sebelumnya gagal atau dijalankan dengan `keep_scope: true`. Hapus tabel `BRONZE_SCOPE_*` secara manual |
| Tabel reference tidak ter-update | Tabel reference sengaja dipakai ulang. Hapus tabel di MinIO/ClickHouse untuk memaksa ambil ulang |
