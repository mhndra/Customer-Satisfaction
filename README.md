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

![alt text](images/data_pipeline)

## 04 Semantic Model
Berdasarkan data pipeline, hasil akhir alur ELT data adalah data warehouse dengan star schema yang disimpan dalam skema gold di Lakehouse_CUstomerSatisfactionAnalytics. Data warehouse yang terdiri dari tabel dimensi dan fakta ini digunakan untuk pembuatan dashboard. Pembuatan dashboard dilakukan dengan menggunakan Fabric Semantic Model untuk data modeling dan pembuatan pengukuran, serta Fabric Report untuk visualisasi data.