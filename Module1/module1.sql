select * from trip_data
select * from zones

-- Question 3. Counting short trips
SELECT 
    COUNT(*)
FROM 
    trip_data
WHERE 
    lpep_pickup_datetime >= '2025-11-01 00:00:00' 
    AND lpep_pickup_datetime < '2025-12-01 00:00:00'
    AND trip_distance <= 1.0;

-- Question 4. Longest trip for each day
SELECT 
	lpep_pickup_datetime, trip_distance
FROM 
    trip_data
WHERE trip_distance < 100
ORDER BY trip_distance DESC
LIMIT 3

-- Question 5. Biggest pickup zone
SELECT 
    z."Zone", 
    SUM(t.total_amount) AS total_revenue
FROM 
    trip_data AS t
INNER JOIN 
    zones AS z ON t."PULocationID" = z."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_revenue DESC
LIMIT 1

-- Question 6. Largest tip
SELECT 
	"Zone"
FROM 
	zones 
WHERE 
"LocationID" = (SELECT
				t."DOLocationID"
				FROM trip_data t
				INNER JOIN zones z
				ON z."LocationID" = t."PULocationID"
				WHERE z."Zone" = 'East Harlem North' AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 11
				ORDER BY t.tip_amount DESC
				LIMIT 1)
