# Customer Satisfaction of an Online Travel Agent App

## 01 Project Objectives
- Masalah: Terdapat keluhan pengguna aplikasi Online Travel Agent (OTA) mengalami hambatan di tiap tahap customer journey dalam aplikasi (Learn, Buy, Get, Use, Pay, Get Help). Namun, keluhan di ulasan app tersebar dan tidak terpetakan per tahap, sehingga perusahan OTA sulit mengetahui di titik mana pengalaman pengguna aplikasi paling mengecewakan.
- Penyelesaian: Pembuatan dashboard proyek ini membantu perusahaan memetakan keluhan pelanggan ke setiap tahap customer journey beserta topiknya, lalu memprioritaskan perbaikan pada tahap dengan keluhan terbanyak dan rating terendah, serta memantau tren rating untuk mengukur hasilnya.
- Tujuan bisnis: Memahami kepuasan pelanggan dengan terukur untuk mendukung keputusan perbaikan berbasis data, demi meningkatkan rating aplikasi dan loyalitas pelanggan.

## 02 Source System
Sumber data yang digunakan berasal dari rating dan ulasan aplikasi OTA di Google Play Store. Pengambilan data ini menggunakan google-play-scraper Python library. Selanjutnya, data disimpan berbentu delta table di Fabric Lakehouse sebagai Bronze layer dalam sebuah alur ELT data. Berikut informasi data tersebut:
- Nama tabel → google_play_reviews
- Deskripsi data → Rating dan ulasan pengguna aplikasi OTA di Google Play Store yang diambil dalam setahun terakhir.
- Bentuk data → Data terstruktur yang telah diekstrak dan dimuat menjadi bentuk delta table dan disimpan dalam skema bronze di Lakehouse_CustomerSatisfaction.
- Pengelompokkan tipe data → Data ini dikelompokkan dalam tiga tipe data: Categorical Data, Date and Time Data, serta Continuous Data untuk konteks dan pengukuran metrik dalam analisis.

## 03 Data Pipeline

![alt text](images/data_pipeline.png)

## 04 Semantic Model
Berdasarkan data pipeline, hasil akhir alur ELT data adalah data warehouse dengan star schema yang disimpan dalam skema gold di Lakehouse_CUstomerSatisfactionAnalytics. Data warehouse yang terdiri dari tabel dimensi dan fakta ini digunakan untuk pembuatan dashboard. Pembuatan dashboard dilakukan dengan menggunakan Fabric Semantic Model untuk data modeling dan pembuatan pengukuran, serta Fabric Report untuk visualisasi data. Berikut informasi tabel dimensi dan fakta yang digunakan:

![alt text](images/star_schema_diagram.png)

- Tabel fact_customer_satisfactions
    - Kolom dan tipe data

        | Nama kolom                | Tipe data     | Pengelompokkan tipe data  |
        |---------------------------|---------------|---------------------------|
        | Customer Satisfaction Key | VARCHAR(8000) | Surrogate key             |
        | Review Key                | VARCHAR(8000) | Foreign key               |
        | User Key                  | VARCHAR(8000) | Foreign key               |
        | App Key                   | VARCHAR(8000) | Foreign key               |
        | Root Cause Key            | VARCHAR(8000) | Foreign key               |
        | Review Date (UTC)         | DATE          | Foreign key               |
        | Satisfaction Score        | BIGINT        | Continuous data/Measure   |
    
    - Kolom referensi

        | Nama kolom referensi  | Tujuan kolom      | Tujuan tabel      |
        |-----------------------|-------------------|-------------------|
        | Review Key            | Review Key        | dim_reviews       |
        | User Key              | User Key          | dim_users         |
        | App Key               | App Key           | dim_apps          |
        | Root Cause Key        | Root Cause Key    | dim_root_causes   |
        | Review Date (UTC)     | Review Date       | dim_review_date   |

- Tabel dim_apps
    - Kolom dan tipe data

        | Nama kolom    | Tipe data     | Pengelompokkan tipe data                          |
        |---------------|---------------|---------------------------------------------------|
        | App Key       | VARCHAR(8000) | Surrogate key                                     |
        | App ID        | VARCHAR(8000) | Natural key/Categorical data/Dimension attribute  |
        | App Version   | VARCHAR(8000) | Categorical data/Dimension attribute              |

- Tabel dim_review_date
    - Kolom dan tipe data

        | Nama kolom            | Tipe data     | Pengelompokkan tipe data                  |
        |-----------------------|---------------|-------------------------------------------|
        | Review Date           | DATE          | Primary key                               |
        | First Day of Week     | DATETIME2(6)  | Date and time data/Dimension attribute    |
        | First Day of Month    | DATETIME2(6)  | Date and time data/Dimension attribute    |
        | Day                   | INT           | Continuous data/Dimension attribute       | 
        | Month                 | VARCHAR(8000) | Categorical data/Dimension attribute      |
        | Month Sorting         | INT           | Continuous data/Dimension attribute       |
        | Quarter               | VARCHAR(8000) | Categorical data/Dimension attribute      |
        | Quarter Sorting       | INT           | Continuous data/Dimension attribute       |
        | Year                  | INT           | Continuous data/Dimension attribute       |
        | Day of Week           | VARCHAR(8000) | Categorical data/Dimension attribute      |
        | Day of Week Sorting   | INT           | Continuous data/Dimension attribute       |

- Tabel dim_reviews
    - Kolom dan tipe data

        | Nama kolom                | Tipe data     | Pengelompokkan tipe data              |
        |---------------------------|---------------|---------------------------------------|
        | Review Key                | VARCHAR(8000) | Surrogate key                         |
        | Review ID                 | VARCHAR(8000) | Categorical data/Dimension attribute  | 
        | Review Created Version    | VARCHAR(8000) | Categorical data/Dimension attribute  | 
        | Review                    | VARCHAR(8000) | Categorical data/Dimension attribute  | 
        | Has Complaint             | BIT           | Categorical data/Dimension attribute  |
        | Primary Theme             | VARCHAR(8000) | Categorical data/Dimension attribute  |

- Tabel dim_root_causes
    - Kolom dan tipe data

        | Nama kolom            | Tipe data     | Pengelompokkan tipe data              |
        |-----------------------|---------------|---------------------------------------|
        | Root Cause Key        | VARCHAR(8000) | Surrogate key                         |
        | Root Cause            | VARCHAR(8000) | Categorical data/Dimension attribute  |
        | Root Cause Weight     | INT           | Continuous data/Dimension attribute   |
        | Cluster ID            | INT           | Continuous data/Dimension attribute   |
        | Cluster Name          | VARCHAR(8000) | Categorical data/Dimension attribute  |
        | Cluster Description   | VARCHAR(8000) | Categorical data/Dimension attribute  |

- Tabel dim_users
    - Kolom dan tipe data

        | Nama kolom    | Tipe data     | Pengelompokkan tipe data              |
        |---------------|---------------|---------------------------------------|
        |User Key       | VARCHAR(8000) | Surrogate key                         |
        |User Name      | VARCHAR(8000) | Categorical data/Dimension attribute  |
        |User Image     | VARCHAR(8000) | Categorical data/Dimension attribute  |