-- Top 20 pickup -> dropoff pairs by trip count (last 30 days)
SELECT
  pickup_location_id,
  dropoff_location_id,
  COUNT(*) AS trips,
  ROUND(AVG(total_amount), 2) AS avg_total_amount
FROM taxi_dwh.gold.fact_taxitrips
WHERE trip_date >= date_sub(current_date(), 30)
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY trips DESC
LIMIT 20;

-- Hourly demand profile
SELECT
  trip_hour,
  COUNT(*) AS trips,
  ROUND(AVG(trip_duration_min), 2) AS avg_duration_min
FROM taxi_dwh.gold.fact_taxitrips
GROUP BY trip_hour
ORDER BY trip_hour;

-- Payment mix by count and revenue
SELECT
  payment_type,
  COUNT(*) AS trips,
  ROUND(SUM(total_amount), 2) AS gross_revenue
FROM taxi_dwh.gold.fact_taxitrips
GROUP BY payment_type
ORDER BY gross_revenue DESC;
