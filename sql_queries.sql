SELECT date,
    postal_code,
    avg_temperature_air_2m_f - LAG(avg_temperature_air_2m_f) OVER(
        PARTITION BY postal_code
        ORDER BY date ASC
    ) AS delta_tempreture_previous_day,
    avg_humidity_relative_2m_pct - LAG(avg_humidity_relative_2m_pct) OVER(
        PARTITION BY postal_code
        ORDER BY date ASC
    ) AS delta_humidity_previous_day
FROM `arched-album-245206.testing.storemeaven`
ORDER BY postal_code,
    date

-- Pull the total amount of confirmed cases and deaths of COVID-19 in France and Germany between March 
-- and May in a monthly granularity. The output should be broken down by GEO

SELECT Extract(
        MONTH
        FROM date
    ) AS month,
    countries_and_territories,
    SUM(daily_confirmed_cases) AS total_confirmed_cases,
    SUM(daily_deaths) AS total_deaths
FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
WHERE country_territory_code IN ('FRA', 'DEU')
    AND EXTRACT(
        MONTH
        FROM date
    ) IN (3, 4, 5)
GROUP BY 1,
    2

-- How many consecutive days we had in 2020 (UK only) in which school and public events were canceled 
-- and we saw a decrease in deaths
WITH daily_death AS (
    SELECT date,
        daily_confirmed_cases,
        daily_deaths,
        confirmed_cases,
        deaths,
        'United Kingdom' AS country_name
    FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
    WHERE countries_and_territories = 'United_Kingdom'
),
cancel_sp AS (
    SELECT DISTINCT date,
        country_name,
        FROM `bigquery-public-data.covid19_govt_response.oxford_policy_tracker`
    WHERE country_name = 'United Kingdom'
        AND CAST(cancel_public_events AS NUMERIC) = 2
        AND CAST(school_closing AS NUMERIC) > 1
    ORDER BY 1 ASC
),
data_set as (
    SELECT d.country_name,
        d.date AS d_date,
        daily_confirmed_cases,
        daily_deaths,
        confirmed_cases as total_comfirmed_cases,
        deaths as total_deaths,
        c.date AS c_date
    FROM daily_death AS d
        LEFT JOIN cancel_sp AS c ON d.date = c.date
        AND d.country_name = c.country_name
    WHERE EXTRACT(
            YEAR
            FROM d.date
        ) = 2020
    ORDER BY d.date ASC
),
-- https://tapoueh.org/blog/2018/02/find-the-number-of-the-longest-continuously-rising-days-for-a-stock/
-- https://learnsql.com/blog/how-to-calculate-length-of-series-in-sql/
-- https://stackoverflow.com/questions/36927685/count-number-of-consecutive-occurrence-of-values-in-table
restric_streak as (
    SELECT *,
        RANK() OVER (
            ORDER BY c_date
        ) AS row_number,
        DATE_ADD(
            c_date,
            INTERVAL - RANK() OVER (
                ORDER BY c_date
            ) DAY
        ) AS date_group
    FROM data_set
    order by d_date asc
),
cancel_sp_result as (
    -- select * from result_set;
    SELECT COUNT(*) AS days_streak,
        IFNULL(CAST(MIN(c_date) AS String), "No Restriction") AS min_date,
        IFNULL(CAST(MAX(c_date) AS String), "No Restriction") AS max_date
    FROM restric_streak
    GROUP BY date_group
),
diffs as (
    SELECT d_date,
        daily_deaths,
        lag(daily_deaths, 1) over(
            order by d_date
        ) as daily_death_previous_day,
        case
            when daily_deaths - lag(daily_deaths, 1) over(
                order by d_date
            ) < 0 then '-'
            when daily_deaths - lag(daily_deaths, 1) over(
                order by d_date
            ) > 0 then '+'
            else '0'
        end as diff
    FROM data_set
    order by d_date asc
),
cte AS (
    SELECT d_date,
        diff,
        SUM(
            CASE
                WHEN diff = s.prev THEN 0
                ELSE 1
            END
        ) OVER(
            ORDER BY d_date
        ) as grp
    FROM (
            SELECT *,
                LAG(diff) OVER(
                    ORDER BY d_date
                ) as prev
            FROM diffs
        ) as s
),
decrease_death as (
    SELECT diff,
        COUNT(*) as cnt,
        min(d_date) as start_date,
        max(d_date) as end_date
    FROM cte
    GROUP BY grp,
        diff
    ORDER BY grp
)
select *
from (
        select *
        from decrease_death
        where diff = '-'
        order by cnt desc
    )
    cross join (
        select *
        from cancel_sp_result
        where min_date not like 'No Restriction'
    )
where start_date >= cast(min_date as date)
    and end_date <= cast(max_date as date)
order by cnt desc



-- Order the top 50 GEOs by their population in 2019 that had a "stay home requirements" during April 2020  
WITH pop AS (
    SELECT DISTINCT country_territory_code,
        pop_data_2019
    FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
),
req AS (
    SELECT distinct country_name,
        alpha_3_code
    FROM `bigquery-public-data.covid19_govt_response.oxford_policy_tracker`
    WHERE CAST(stay_at_home_requirements AS NUMERIC) > 0
        AND EXTRACT(
            MONTH
            FROM date
        ) = 4
)
SELECT *
FROM pop AS p
    LEFT JOIN req AS r ON p.country_territory_code = r.alpha_3_code
where r.country_name is not null
ORDER BY p.pop_data_2019 DESC
Limit 50


-- In how many 2020 dates have we seen a daily confirmed dead cases which are bigger than 100 
-- (in the US only), and a positive change from baseline of grocery and pharmacy mobility rate (in the US only)

WITH us_gp_mob AS (
    SELECT date,
        'USA' AS country_territory_code,
        AVG(
            grocery_and_pharmacy_percent_change_from_baseline
        ) AS avg_grocery_and_pharmacy_percent_change_from_baseline
    FROM `bigquery-public-data.covid19_google_mobility.mobility_report`
    WHERE country_region_code = 'US'
    GROUP BY 1,
        2
    ORDER BY 1 ASC
),
us_dead_cases AS (
    SELECT date,
        country_territory_code,
        SUM(daily_deaths) AS daily_confirmed_deaths
    FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
    WHERE country_territory_code = 'USA'
    GROUP BY date,
        country_territory_code
    HAVING SUM(daily_deaths) > 100
    ORDER BY 1 ASC
)
SELECT COUNT(1) AS number_of_days
FROM us_dead_cases AS ud
    LEFT JOIN us_gp_mob AS ugp ON ud.date = ugp.date
    AND ud.country_territory_code = ugp.country_territory_code
WHERE ugp.avg_grocery_and_pharmacy_percent_change_from_baseline > 0

--`bigquery-public-data.covid19_google_mobility.mobility_report`
--`bigquery-public-data.covid19_govt_response.oxford_policy_tracker`
--`bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
