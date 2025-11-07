# ETL-Pipeline-with-Python-Operator
# ETL Pipeline untuk Open-Meteo API

## Deskripsi
Proyek ini adalah pipeline ETL (Extract → Transform → Load) yang mengambil data cuaca dari **Open-Meteo API**, mentransformasikan data tersebut, dan menyimpannya ke dalam **PostgreSQL** serta **CSV** (sebagai backup). Pipeline ini diatur menggunakan **Apache Airflow** untuk orkestrasi tugas.

## Struktur Proyek
ETL_PROJECT/
├─ dags/ # Folder untuk DAG Airflow
│ └─ weather_etl.py # DAG utama yang mengorkestrasi ETL
├─ ConnectUpsert/ # Folder untuk koneksi dan UPSERT ke Postgres
│ ├─ init.py # File init untuk package
│ ├─ connect_postgres.py # Koneksi ke PostgreSQL menggunakan PostgresHook
│ └─ upsert_postgres.py # Fungsi UPSERT ke Postgres
├─ data/ # Folder untuk file data (CSV dan staging)
│ ├─ landing/ # Data raw JSON (hasil ekstraksi)
│ └─ staging/ # Data CSV hasil transformasi
├─ etl_weather/ # Modul untuk ETL (Extract → Transform → Load)
│ ├─ init.py # File init untuk package
│ ├─ config.py # File konfigurasi (misalnya untuk lat, lon, timezone)
│ ├─ extract.py # Fungsi untuk ekstraksi data dari API Open-Meteo
│ ├─ transform.py # Fungsi untuk transformasi data (data cleaning)
│ ├─ load.py # Fungsi untuk menyimpan ke CSV atau Postgres
└─ requirements.txt # Daftar dependensi Python (requests, pandas, etc.)


## Cara Menjalankan Proyek

### 1. Instalasi Dependencies
Instal semua dependencies yang diperlukan dengan menjalankan perintah berikut di terminal:

```bash
pip install -r requirements.txt

# Membuat database Airflow (jika belum ada)
airflow db init

# Menjalankan web server Airflow
airflow webserver -p 8080

# Contoh:
Menambahkan Koneksi dan Variabel di Airflow

Masukkan koneksi ke PostgreSQL melalui Admin → Connections di UI Airflow:

Conn Id: postgres_default (atau sesuai dengan yang kamu set di PG_CONN_ID)

Conn Type: Postgres

Host: localhost (atau host Postgres kamu)

Schema: weather_db (atau sesuai dengan database yang digunakan)

Login: postgres (atau username yang kamu set)

Password: postgres (atau password yang sesuai)

Port: 5432 (port default PostgreSQL)

Tambahkan variabel PG_CONN_ID dan PG_TABLE di Admin → Variables:

PG_CONN_ID: postgres_default

PG_TABLE: public.weather_hourly

### Penjelasan:

1. **Bagian Struktur Proyek**: Menjelaskan bagaimana proyek ini diatur, dengan folder **`dags/`** untuk DAG utama dan folder lainnya untuk **koneksi**, **upsert**, dan **ETL**.
2. **Cara Menjalankan Proyek**: Memberikan instruksi langkah demi langkah tentang cara menginstal dependensi, menyiapkan Airflow, menambahkan koneksi, dan menjalankan DAG.
3. **Verifikasi Output**: Menyediakan langkah-langkah untuk memverifikasi bahwa data berhasil dimuat ke PostgreSQL dan CSV.
4. **Catatan**: Memberikan pengingat untuk memeriksa dan memastikan **koneksi** dan **tabel** di Postgres sudah benar, serta hak akses untuk folder **data/**.
5. **Pengujian dan Debugging**: Memberikan tips untuk menguji setiap task secara terpisah dan cara memeriksa log di Airflow untuk debugging.

---

Dengan **`README.md`** ini, siapapun yang ingin menjalankan proyek ini di komputernya akan bisa mengikuti langkah-langkah yang jelas dan terstruktur. Jika ada bagian yang ingin ditambahkan atau disesuaikan, beri tahu saya!