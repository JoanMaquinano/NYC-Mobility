-- Gold — dim_date

SET TIME ZONE 'UTC';
USE CATALOG nyc_mobility;


-- # 1. The bounds
--
-- ## Two defences, because one was not enough
--
-- **First: read `vw_green_taxi_valid`, not the table.** Silver keeps its FAIL
-- rows rather than deleting them, so the raw MIN and MAX over
-- `green_taxi_clean` are whatever garbage dates the source carried. The view
-- is `dq_status IN ('PASS', 'WARN')` -- the same rows a `dq_status <> 'FAIL'`
-- filter would give, but stated once, in one place, with a COMMENT that says
-- "What Gold reads". Change the definition of valid later and this follows.
--
-- That alone was not enough on the first attempt. The span came back 6,300
-- days, 2009-01-01 to 2026-04-01, because Silver's era rule was
-- `out_of_era_pickups_are_quarantined` -- PICKUPS, against a hardcoded 2009
-- literal. A row with a sane 2026 pickup and a corrupt 2009 dropoff passed
-- it, kept dq_status PASS, and still set the bound, because the bounds take
-- LEAST over pickup AND dropoff. One row, sixteen years of calendar.
--
-- Silver now derives that window from each row's own `source_file` month and
-- checks both timestamps, so such a row is FAIL and the view excludes it.
--
-- **Second defence: clamp to the weather window.** Trip dates outside
-- [weather_min - 7, weather_max + 7] are ignored when computing the bounds.
-- The weather feed is fetched per month with a known range, so it is the
-- trustworthy anchor here; the taxi dates are the ones carrying source junk.
--
-- Seven days of margin: generous enough for a trip that starts near a
-- boundary and for the UTC-vs-NY offset at the month edges, tight enough that
-- a 2009 date has no chance. It is a sanity clamp, not a business rule.
--
-- `trip_dates_excluded` reports how many were ignored, so the clamp is never
-- silent. A number that climbs means the source is getting worse, or the
-- weather window no longer covers the trips.
--
-- **This does not drop any trips.** It only decides how wide the calendar is.
--
-- Note the deliberate asymmetry: dim_date builds from the VALID view, while
-- the fact_taxi_trip MERGE reads `green_taxi_clean` whole and carries
-- `qc_error_descriptions` across. The calendar is built from trips worth
-- counting; the fact keeps everything and labels it. So a quarantined trip
-- keeps its fact row, its date falls outside the calendar, and it surfaces in
-- `pickup_date_resolves` at at-rest -- which is where an unresolvable foreign
-- key belongs, and why that check is tolerated at 10% rather than strict.

CREATE OR REPLACE TEMPORARY VIEW vw_dim_date_bounds AS
WITH wb AS (
    SELECT MIN(to_date(weather_hour)) AS w_min,
           MAX(to_date(weather_hour)) AS w_max
    FROM   nyc_silver.vw_weather_valid
    WHERE  weather_hour IS NOT NULL
),
-- Pickup AND dropoff: a trip starting at 23:50 on the last day ends on the
-- next one, and dropoff_date is a foreign key too.
trip_dates AS (
    SELECT DATE(lpep_pickup_datetime) AS d
    FROM   nyc_silver.vw_green_taxi_valid
    WHERE  lpep_pickup_datetime IS NOT NULL
    UNION ALL
    SELECT DATE(lpep_dropoff_datetime)
    FROM   nyc_silver.vw_green_taxi_valid
    WHERE  lpep_dropoff_datetime IS NOT NULL
),
t AS (
    SELECT MIN(d) AS t_min, MAX(d) AS t_max
    FROM   trip_dates
    WHERE  d BETWEEN date_sub((SELECT w_min FROM wb), 7)
               AND   date_add((SELECT w_max FROM wb), 7)
),
x AS (
    SELECT COUNT(*) AS n_excluded
    FROM   trip_dates
    WHERE  d NOT BETWEEN date_sub((SELECT w_min FROM wb), 7)
                   AND   date_add((SELECT w_max FROM wb), 7)
)
SELECT
    LEAST(w_min, t_min)                                        AS date_from,
    GREATEST(w_max, t_max)                                     AS date_to,
    datediff(GREATEST(w_max, t_max), LEAST(w_min, t_min)) + 1  AS span_days,
    w_min, w_max, t_min, t_max,
    (SELECT n_excluded FROM x)                                 AS trip_dates_excluded
FROM   wb, t;


SELECT date_from, date_to, span_days,
       w_min AS weather_from, w_max AS weather_to,
       t_min AS trips_from,   t_max AS trips_to,
       trip_dates_excluded
FROM   vw_dim_date_bounds;


-- ## Weather has to exist for the clamp to mean anything
--
-- If weather_clean is empty, w_min is NULL, the BETWEEN matches nothing, and
-- the bounds come back NULL -- which sequence() turns into an unhelpful
-- error. Say so plainly instead.
SELECT CASE
    WHEN (SELECT w_min FROM vw_dim_date_bounds) IS NULL
      THEN raise_error('nyc_silver.weather_clean has no usable weather_hour, '
                    || 'so the calendar bounds cannot be anchored. Load weather '
                    || 'before building dim_date.')
    ELSE 'weather window present' END AS precondition_weather;


-- ## The trip dates the clamp excluded
--
-- Deliberately unfiltered on dq_status -- the job of this query is to SHOW
-- what the bounds now ignore, and whether Silver caught it.
--
-- Every row here reading FAIL means Silver already knows. A row reading PASS
-- or WARN is the interesting one: the classification rules have a gap, and
-- the clamp is the only thing catching it. The 2009 dropoff is exactly that
-- case -- worth adding a dropoff era rule in Silver.

SELECT 'pickup' AS side, DATE(lpep_pickup_datetime) AS d, dq_status,
       COUNT(*) AS rows, MIN(source_file) AS example_file
FROM   nyc_silver.green_taxi_clean
WHERE  lpep_pickup_datetime IS NOT NULL
  AND  DATE(lpep_pickup_datetime) NOT BETWEEN
       (SELECT date_sub(w_min, 7) FROM vw_dim_date_bounds)
   AND (SELECT date_add(w_max, 7) FROM vw_dim_date_bounds)
GROUP  BY 1, 2, 3
UNION ALL
SELECT 'dropoff', DATE(lpep_dropoff_datetime), dq_status,
       COUNT(*), MIN(source_file)
FROM   nyc_silver.green_taxi_clean
WHERE  lpep_dropoff_datetime IS NOT NULL
  AND  DATE(lpep_dropoff_datetime) NOT BETWEEN
       (SELECT date_sub(w_min, 7) FROM vw_dim_date_bounds)
   AND (SELECT date_add(w_max, 7) FROM vw_dim_date_bounds)
GROUP  BY 1, 2, 3
ORDER  BY d, side;


-- ## A guard on the span
--
-- `sequence()` builds the whole array in memory before `explode` unpacks it,
-- so a corrupted year would ask for millions of rows and fail with an
-- out-of-memory error rather than anything naming its cause.
--
-- 1827 days (5 years), down from 18262 (50 years). The old guard was loose
-- enough to let a 6,301-day calendar through in silence.
--
-- With the clamp above this should never fire. If it does, the weather window
-- itself is wrong -- check `weather_from` / `weather_to` in the first result.
-- Do not widen the guard.

SELECT CASE WHEN span_days > 1827
    THEN raise_error(CONCAT(
           'dim_date span is ', CAST(span_days AS STRING), ' days (',
           CAST(date_from AS STRING), ' to ', CAST(date_to AS STRING),
           ') -- over 5 years, with the weather clamp applied. That means the ',
           'weather window is wrong, not the taxi dates. Check weather_from / ',
           'weather_to above; do not widen this guard.'))
    ELSE CONCAT('OK - building ', CAST(span_days AS STRING), ' days from ',
                CAST(date_from AS STRING), ' to ', CAST(date_to AS STRING))
END AS precondition_span
FROM vw_dim_date_bounds;


-- # 2. Build
--
-- Unchanged. Every attribute is a pure function of `full_date`, which is a
-- DATE -- no time, no zone -- so date_key and all the parts are stable in any
-- session. That is why dim_date was the only Gold table with zero failures
-- when the zones disagreed.
--
-- `DAYOFWEEK()` is 1 = Sunday .. 7 = Saturday in Spark, NOT ISO. The weekend
-- test is IN (1, 7); written IN (6, 7) it gives Friday and Saturday.

MERGE INTO nyc_mobility.nyc_gold.dim_date AS target
USING (
  SELECT
    CAST(DATE_FORMAT(full_date, 'yyyyMMdd') AS INT) AS date_key,
    full_date,
    YEAR(full_date)                                 AS year,
    QUARTER(full_date)                              AS quarter,
    MONTH(full_date)                                AS month,
    MONTHNAME(full_date)                            AS month_name,
    DAYOFWEEK(full_date)                            AS day_of_week,
    DAYNAME(full_date)                              AS day_name,
    DAYOFWEEK(full_date) IN (1, 7)                  AS is_weekend
  FROM (
    SELECT explode(sequence(date_from, date_to, INTERVAL 1 DAY)) AS full_date
    FROM   vw_dim_date_bounds
  ) calendar
) AS source
ON target.date_key <=> source.date_key
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
