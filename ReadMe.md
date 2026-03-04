# 💳 Fintech Card Portfolio & Fraud Analysis (GCP & BigQuery)

## 🎯 Cel projektu

Projekt został stworzony w celu demonstracji umiejętności analitycznych w środowisku chmurowym **Google Cloud Platform (BigQuery)**. Analiza skupia się na portfelu kart płatniczych, segmentacji klientów pod kątem limitów kredytowych oraz wykrywaniu anomalii finansowych – kluczowych obszarach dla sektora Fintech (np. Tpay).

---

## 🛠️ Wykorzystane Technologie

| Obszar | Technologia |
|---|---|
| Hurtownia Danych | Google BigQuery |
| Język | Standard SQL |
| Statystyka & ML | BigQuery ML (K-means Clustering) |
| Automatyzacja | Python (Kaggle API, ETL process) |

---

## 📊 Analiza SQL

### 1. Ranking Marek i Wydajność Portfela

Analiza segmentuje karty według brandu i typu, wyliczając średnie limity oraz prognozując wygasanie produktów.

**Kluczowe funkcje:** `RANK()`, `AVG() OVER()`, `COUNTIF()`
```sql
WITH card_metrics AS (
  SELECT 
    card_brand, card_type, credit_limit,
    CAST(SUBSTR(expires, 4, 4) AS INT64) as expiry_year
  FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`
)
SELECT 
  card_brand, card_type,
  COUNT(*) as card_count,
  ROUND(AVG(credit_limit), 2) as avg_actual_limit,
  COUNTIF(expiry_year <= 2025) as cards_expiring_soon,
  RANK() OVER(ORDER BY AVG(credit_limit) DESC) as brand_rank
FROM card_metrics
GROUP BY 1, 2
ORDER BY brand_rank;
```

>  <img width="995" height="241" alt="image" src="https://github.com/user-attachments/assets/da16808d-7f65-46d1-826d-e5492a087a0b" />

> <img width="1332" height="385" alt="image" src="https://github.com/user-attachments/assets/7fab1758-57d4-4875-a0fb-738cd50dddbd" />


---

### 2. Analiza Skumulowana Wygasania (Churn Prediction)

Zapytanie pozwala przewidzieć obciążenie operacyjne związane z wymianą kart w nadchodzących latach.
```sql
WITH base_expiry AS (
  SELECT 
    card_brand,
    CAST(SUBSTR(expires, 4, 4) AS INT64) as expiry_year,
    COUNT(*) as cards_in_year
  FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`
  WHERE expires IS NOT NULL
  GROUP BY 1, 2
)
SELECT 
  card_brand, expiry_year, cards_in_year,
  SUM(cards_in_year) OVER(PARTITION BY card_brand ORDER BY expiry_year) as cumulative_expired_total
FROM base_expiry
ORDER BY card_brand, expiry_year;
```
> <img width="748" height="342" alt="image" src="https://github.com/user-attachments/assets/ee87741a-d9e4-47c5-86af-9fa7e72947c2" />

> <img width="1335" height="396" alt="image" src="https://github.com/user-attachments/assets/a2224ac9-32a0-4d35-8da6-09c1305ee072" />

---

### 3. Detekcja Anomalii (Z-Score Analysis)

Wykrywanie rekordów, których limit kredytowy odbiega od średniej o ponad **2 odchylenia standardowe** – istotne dla działów Risk & Fraud.
```sql
WITH global_stats AS (
  SELECT 
    AVG(credit_limit) as avg_limit_all,
    STDDEV(credit_limit) as stddev_limit_all
  FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`
)
SELECT 
  t.client_id, t.card_brand, t.credit_limit,
  ROUND((t.credit_limit - g.avg_limit_all) / g.stddev_limit_all, 2) as z_score
FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions` t, global_stats g
WHERE (t.credit_limit - g.avg_limit_all) / g.stddev_limit_all > 2
ORDER BY z_score DESC;
```

> <img width="626" height="368" alt="image" src="https://github.com/user-attachments/assets/0cb63e3d-ff65-429d-9779-dcf4e351daa1" />

> <img width="1292" height="380" alt="image" src="https://github.com/user-attachments/assets/b16dcff9-b0d9-4f58-b230-4f02d73c0968" />



---

## 🤖 Machine Learning (BigQuery ML)

Zaimplementowano model klastrowania **K-means** do automatycznej segmentacji klientów.

```sql
CREATE OR REPLACE MODEL `tpay_fraud_project.customer_segments`
OPTIONS(model_type='kmeans', num_clusters=3) AS
SELECT 
  credit_limit, 
  num_cards_issued, 
  IF(has_chip, 1, 0) as chip_flag
FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`;
```

### Ewaluacja modelu

| Metryka | Wynik |
|---|---|
| Indeks Daviesa-Bouldina | 1.29 *(Silna separacja klastrów)* |
| Odległość średniokwadratowa | 1.64 |

### Wyniki ML.PREDICT

Poniższa tabela przedstawia charakterystykę zidentyfikowanych segmentów:

| centroid_id | avg_limit | avg_cards | count_of_clients | Business Segment |
|---|---|---|---|---|
| 2 | *39609.01* | *1.5* | *601* | 🏆 High-Value (VIP) |
| 1 | *12643.85* | *1.0* | *2835* | 👤 Standard User |
| 3 | *10527.44* | *2.0* | *2710* | 📦 Entry Level / Mass Market |

---

## 💡 Wnioski

1. **Segmentacja:** Zidentyfikowano kluczowe grupy klientów, co pozwala na personalizację limitów kredytowych.
2. **Ryzyko:** Dzięki analizie Z-score wytypowano transakcje o podwyższonym ryzyku, które wymagają dodatkowej weryfikacji.
3. **Optymalizacja:** Wykazano trend wygasania kart, co umożliwia optymalizację łańcucha dostaw dla nowych kart.
