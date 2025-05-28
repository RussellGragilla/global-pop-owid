-- Preliminary observation of the raw data --

SELECT *
FROM public.population;


-- Ascertaining what is the most recent year for the data collected in this dataset --

SELECT *
FROM public.population
ORDER BY "Year" DESC;


-- Filtering the data on only the most recent year --

SELECT *
FROM public.population
WHERE "Year" = 2023;


-- Organizing the data so the countries with the highest populations come first, only for the year 2023 --

SELECT *
FROM public.population
WHERE "Year" = 2023
ORDER BY "Population" DESC;


-- Removing regions from the data, so that the dataset only contains specific countries --

SELECT "Entity", "Year", "Population"
FROM public.population
WHERE "Year" = 2023
  AND TRIM("Entity") NOT IN ('World', 
  						 	 'Less developed regions', 
						 	 'Less developed regions, excluding least developed countries', 
						  	 'Less developed regions, excluding China',
							 'Asia (UN)',
							 'Lower-middle-income countries',
							 'Upper-middle-income countries',
							 'Africa (UN)',
							 'More developed regions',
							 'High-income countries',
							 'Least developed countries',
							 'Americas (UN)',
							 'Europe (UN)',
							 'Low-income countries',
							 'Latin America and the Caribbean (UN)',
							 'Land-locked developing countries (LLDC)',
							 'Northern America (UN)',
							 'Small island developing states (SIDS)',
							 'Oceania (UN)')
ORDER BY "Population" DESC;


-- Creating a temp table with only distinct countries in the Entity column, as well as new columns to draw on --

DROP TABLE IF EXISTS poptemp;
CREATE TEMPORARY TABLE poptemp (
	"Entity" TEXT,
	"Year" SMALLINT,
	"Population" BIGINT,
	"Lag Population" BIGINT,
	"YoY Growth %" NUMERIC
);


-- Inserting the filtered country data into the new table, as well as the new columns --

INSERT INTO poptemp
WITH cte_table AS (
	SELECT *,
		LAG("Population") OVER (PARTITION BY "Entity" ORDER BY "Year") AS "Lag Population"
	FROM public.population
	WHERE TRIM("Entity") NOT IN ('World', 
  						 	 'Less developed regions', 
						 	 'Less developed regions, excluding least developed countries', 
						  	 'Less developed regions, excluding China',
							 'Asia (UN)',
							 'Lower-middle-income countries',
							 'Upper-middle-income countries',
							 'Africa (UN)',
							 'More developed regions',
							 'High-income countries',
							 'Least developed countries',
							 'Americas (UN)',
							 'Europe (UN)',
							 'Low-income countries',
							 'Latin America and the Caribbean (UN)',
							 'Land-locked developing countries (LLDC)',
							 'Northern America (UN)',
							 'Small island developing states (SIDS)',
							 'Oceania (UN)')
)
SELECT *,
	ROUND(100 * ("Population" - "Lag Population")::DECIMAL / NULLIF("Lag Population", 0), 2) AS "YoY Growth %"
FROM cte_table;


-- Quering the new table, just to verify that it turned out as intended -- 

SELECT *
FROM poptemp
WHERE "Year" = 2023
ORDER BY "Population" DESC;


-- Adding a series of new columns, to illustrate interesting information the data has --

SELECT *,
	ROUND(100.0 * "Population" / SUM("Population") OVER (PARTITION BY "Year"), 2) AS "Global Pop Share %",
	RANK() OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Pop Rank",
	ROUND(100 * PERCENT_RANK() OVER (PARTITION BY "Year" ORDER BY "Population")::DECIMAL, 2) AS "Percentile Rank",
	ROUND(("Population" - AVG("Population") OVER (PARTITION BY "Year")) / 
	NULLIF(STDDEV_POP("Population") OVER (PARTITION BY "Year"),0), 4) AS "Z Score",
	NTILE(4) OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Population Quartile",
	CASE WHEN "YoY Growth %" > AVG("YoY Growth %") OVER (PARTITION BY "Year") THEN True ELSE False END AS "Growth Faster Than Avg",
	CASE WHEN "Population" >= 1000000000 THEN 'Mega'
		 WHEN "Population" >= 100000000 THEN 'Large'
		 WHEN "Population" >= 10000000 THEN 'Medium'
		 ELSE 'Small'
	END AS "Size Class"
FROM poptemp
ORDER BY "Entity", "Year";





DROP TABLE IF EXISTS countrytable;
CREATE TABLE countrytable (
	"Entity" TEXT,
	"Year" SMALLINT,
	"Population" BIGINT,
	"Lag Population" BIGINT,
	"YoY Growth %" NUMERIC,
	"Global Pop Share %" NUMERIC,
	"Pop Rank" BIGINT,
	"Percentile Rank" NUMERIC,
	"Z Score" NUMERIC,
	"Population Quartile" INTEGER,
	"Growth Faster Than Avg" BOOLEAN,
	"Size Class" TEXT
);


INSERT INTO public.countrytable
SELECT *,
	ROUND(100.0 * "Population" / SUM("Population") OVER (PARTITION BY "Year"), 2) AS "Global Pop Share %",
	RANK() OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Pop Rank",
	ROUND(100 * PERCENT_RANK() OVER (PARTITION BY "Year" ORDER BY "Population")::DECIMAL, 2) AS "Percentile Rank",
	ROUND(("Population" - AVG("Population") OVER (PARTITION BY "Year")) / 
	NULLIF(STDDEV_POP("Population") OVER (PARTITION BY "Year"),0), 4) AS "Z Score",
	NTILE(4) OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Population Quartile",
	CASE WHEN "YoY Growth %" > AVG("YoY Growth %") OVER (PARTITION BY "Year") THEN True ELSE False END AS "Growth Faster Than Avg",
	CASE WHEN "Population" >= 1000000000 THEN 'Mega'
		 WHEN "Population" >= 100000000 THEN 'Large'
		 WHEN "Population" >= 10000000 THEN 'Medium'
		 ELSE 'Small'
	END AS "Size Class"
FROM poptemp
ORDER BY "Entity", "Year";



-- CREATING A NEW TABLE TO MIMIC THE OLD TABLE, BUT THIS TIME IT CONTAINS NO COUNTRIES, AND ONLY REGION DATA -- 


DROP TABLE IF EXISTS regiondata;
CREATE TABLE regiondata (
	"Entity" TEXT,
	"Year" SMALLINT,
	"Population" BIGINT,
	"Lag Population" BIGINT,
	"YoY Growth %" NUMERIC,
	"Global Pop Share %" NUMERIC,
	"Pop Rank" BIGINT,
	"Percentile Rank" NUMERIC,
	"Z Score" NUMERIC,
	"Population Quartile" INTEGER,
	"Growth Faster Than Avg" BOOLEAN,
	"Size Class" TEXT
);

-- The previous queries are all combined into one, to populate the new table with the same columns as the previous one, for regions --

WITH cte_table2 AS (
	SELECT *,
		LAG("Population") OVER (PARTITION BY "Entity" ORDER BY "Year") AS "Lag Population"
	FROM public.population
	WHERE TRIM("Entity") IN ('World', 
							 'Less developed regions', 
							 'Less developed regions, excluding least developed countries', 
							 'Less developed regions, excluding China',
							 'Asia (UN)',
							 'Lower-middle-income countries',
							 'Upper-middle-income countries',
							 'Africa (UN)',
							 'More developed regions',
							 'High-income countries',
							 'Least developed countries',
							 'Americas (UN)',
							 'Europe (UN)',
							 'Low-income countries',
							 'Latin America and the Caribbean (UN)',
							 'Land-locked developing countries (LLDC)',
							 'Northern America (UN)',
							 'Small island developing states (SIDS)',
							 'Oceania (UN)')
),
cte_growth AS (
	SELECT *,
		ROUND(100 * ("Population" - "Lag Population")::DECIMAL / NULLIF("Lag Population", 0), 2) AS "YoY Growth %"
	FROM cte_table2
),
cte_metrics AS (
	SELECT *,
		ROUND(100.0 * "Population" / SUM("Population") OVER (PARTITION BY "Year"), 2) AS "Global Pop Share %",
		RANK() OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Pop Rank",
		ROUND(100 * PERCENT_RANK() OVER (PARTITION BY "Year" ORDER BY "Population")::DECIMAL, 2) AS "Percentile Rank",
		ROUND(("Population" - AVG("Population") OVER (PARTITION BY "Year")) / 
			  NULLIF(STDDEV_POP("Population") OVER (PARTITION BY "Year"), 0), 4) AS "Z Score",
		NTILE(4) OVER (PARTITION BY "Year" ORDER BY "Population" DESC) AS "Population Quartile"
	FROM cte_growth
),
cte_final AS (
	SELECT *,
		CASE 
			WHEN "YoY Growth %" > AVG("YoY Growth %") OVER (PARTITION BY "Year") THEN True 
			ELSE False 
		END AS "Growth Faster Than Avg",
		CASE 
			WHEN "Population" >= 1000000000 THEN 'Mega'
			WHEN "Population" >= 100000000 THEN 'Large'
			WHEN "Population" >= 10000000 THEN 'Medium'
			ELSE 'Small'
		END AS "Size Class"
	FROM cte_metrics
)
INSERT INTO public.regiondata
SELECT *
FROM cte_final
ORDER BY "Entity", "Year";
