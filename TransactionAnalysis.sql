-- 1 Zapytanie : Ranking kart
WITH card_metrics AS (
  SELECT 
    card_brand,
    card_type,
    credit_limit,
    has_chip,
    -- Parsowanie daty wygaśnięcia
    CAST(SUBSTR(expires, 4, 4) AS INT64) as expiry_year,
    -- Średni limit dla danego typu karty (np. czy Visa Gold ma wyższe limity niż Silver?)
    AVG(credit_limit) OVER(PARTITION BY card_type) as avg_limit_per_type
  FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`
)
SELECT 
  card_brand,
  card_type,
  COUNT(*) as card_count,
  ROUND(AVG(credit_limit), 2) as avg_actual_limit,
  -- Ile kart wygasa w najbliższym czasie?
  COUNTIF(expiry_year <= 2025) as cards_expiring_soon,
  -- Ranking marek według najwyższego średniego limitu
  RANK() OVER(ORDER BY AVG(credit_limit) DESC) as brand_rank
FROM card_metrics
GROUP BY 1, 2
ORDER BY brand_rank;

-- 2 Zapytanie : Wygaśnięcię kard dla Brandu i poszczególnych lat
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
  card_brand,
  expiry_year,
  cards_in_year,
  SUM(cards_in_year) OVER(PARTITION BY card_brand ORDER BY expiry_year) as cumulative_expired_total
FROM base_expiry
ORDER BY card_brand, expiry_year;

-- 3 Zapytanie: Anomalie
WITH global_stats AS (
  SELECT 
    AVG(credit_limit) as avg_limit_all,
    STDDEV(credit_limit) as stddev_limit_all
  FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions`
)
SELECT 
  t.client_id,
  t.card_brand,
  t.credit_limit,
  g.avg_limit_all,
  -- Obliczamy o ile odchyleń standardowych limit klienta odbiega od średniej
  ROUND((t.credit_limit - g.avg_limit_all) / g.stddev_limit_all, 2) as z_score
FROM `project-18e188c2-2177-4abb-b5d.tpay_fraud_project.transactions` t, global_stats g
WHERE (t.credit_limit - g.avg_limit_all) / g.stddev_limit_all > 2 -- Szukamy anomalii (powyżej 2 odchyleń)
ORDER BY z_score DESC;

