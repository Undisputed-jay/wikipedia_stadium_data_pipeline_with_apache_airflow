-- What is the largest stadium by seating capacity in each country?
SELECT country, stadium, MAX(CAST(REPLACE(seating_capacity, ',', '') AS INTEGER)) AS largest_capacity
FROM stadiums
GROUP BY country, stadium
ORDER BY largest_capacity DESC;

-- Which regions have the highest average seating capacity for stadiums?
SELECT region, AVG(CAST(REPLACE(seating_capacity, ',', '') AS INTEGER)) AS avg_capacity
FROM stadiums
GROUP BY region
ORDER BY avg_capacity DESC;

-- What is the total seating capacity of all stadiums in a specific country?
SELECT country, SUM(CAST(REPLACE(seating_capacity, ',', '') AS INTEGER)) AS total_capacity
FROM stadiums
WHERE country = 'Brazil'
GROUP BY country;

-- Which cities host the most football stadiums?
SELECT city, COUNT(stadium) AS stadium_count
FROM stadiums
GROUP BY city
ORDER BY stadium_count DESC;

-- What is the smallest stadium by seating capacity, and which team uses it?
SELECT stadium, seating_capacity, home_team
FROM stadiums
ORDER BY CAST(REPLACE(seating_capacity, ',', '') AS INTEGER) ASC
LIMIT 1;

-- What are the top 5 countries with the largest average stadium capacities?
SELECT country, AVG(CAST(REPLACE(seating_capacity, ',', '') AS INTEGER)) AS avg_capacity
FROM stadiums
GROUP BY country
ORDER BY avg_capacity DESC
LIMIT 5;
