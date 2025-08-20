-- Basic Queries
SELECT * FROM complaints;
SELECT * FROM residents;
SELECT * FROM service_categories;
SELECT * FROM status_logs;

-- Query 1 | Number of complaints per ward
SELECT r.ward, COUNT(c.complaint_id) AS total_complaints
FROM complaints c
JOIN residents r ON c.resident_id = r.resident_id
GROUP BY r.ward
ORDER BY total_complaints DESC;

-- Make the total complaints per ward vary more

-- Query 2 | Number of complaints per catergory
SELECT sc.category_name, COUNT(c.complaint_id) AS total_complaints
FROM complaints c
JOIN service_categories sc ON c.category_id = sc.category_id
GROUP BY sc.category_name
ORDER BY total_complaints DESC;

-- Add more variance to the total complaints per category

-- Query 3 | Average turnaround time in days from submission to resolution
SELECT 
    c.complaint_id, 
    MIN(sl.status_date) AS submitted_date,
    MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) AS resolved_date,
    MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) 
        - MIN(sl.status_date) AS turnaround_days
FROM status_logs sl
JOIN complaints c ON sl.complaint_id = c.complaint_id
GROUP BY c.complaint_id
HAVING MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) IS NOT NULL;

-- Add more variance to the turnaround days

-- Query 4 | Average turnaround time by category
WITH complaint_turnaround AS (
    SELECT 
        c.complaint_id,
        c.category_id,
        MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) 
            - MIN(sl.status_date) AS turnaround_days
    FROM complaints c
    JOIN status_logs sl ON c.complaint_id = sl.complaint_id
    GROUP BY c.complaint_id, c.category_id
    HAVING MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) IS NOT NULL
)
SELECT 
    sc.category_name,
    ROUND(AVG(ct.turnaround_days)) AS avg_turnaround_days
FROM complaint_turnaround ct
JOIN service_categories sc ON ct.category_id = sc.category_id
GROUP BY sc.category_name
ORDER BY avg_turnaround_days;

-- Add more variance to the avg turnaround days

-- Query 5 | Top 5 residents with the most complaints
SELECT r.first_name || ' ' || r.last_name AS resident_name,
       COUNT(c.complaint_id) AS total_complaints
FROM complaints c
JOIN residents r ON c.resident_id = r.resident_id
GROUP BY resident_name
ORDER BY total_complaints DESC
LIMIT 5;

-- Add more variance to the total complaints of residents 

-- Query 6️ | Complaints still in progress || not displaying any data
SELECT 
    c.complaint_id, 
    c.title, 
    r.first_name || ' ' || r.last_name AS resident_name,
    sl.status AS current_status
FROM complaints c
JOIN residents r ON c.resident_id = r.resident_id
JOIN status_logs sl ON c.complaint_id = sl.complaint_id
WHERE sl.status = 'In Progress'
  AND sl.status_date = (
      SELECT MAX(sl2.status_date) 
      FROM status_logs sl2 
      WHERE sl2.complaint_id = c.complaint_id
  );

-- Query 7 | Number of resolved vs unresolved complaints
SELECT CASE 
           WHEN EXISTS (SELECT 1 FROM status_logs sl2 
                        WHERE sl2.complaint_id = c.complaint_id 
                        AND sl2.status = 'Resolved') 
                THEN 'Resolved'
           ELSE 'Unresolved'
       END AS status,
       COUNT(*) AS total
FROM complaints c
GROUP BY status;

-- Add more variance to the resolved and unresolved cases

-- Query 8 | Monthly complaint trend
SELECT TO_CHAR(c.submission_date, 'YYYY-MM') AS month,
       COUNT(c.complaint_id) AS total_complaints
FROM complaints c
GROUP BY month
ORDER BY month;

-- Add more variance to the monthly complaint trend

-- Query 9️ | Average number of days each status lasts
SELECT status, 
       ROUND(AVG(next_status_date - status_date)) AS avg_days
FROM (
    SELECT sl.complaint_id, sl.status, sl.status_date,
           LEAD(sl.status_date) OVER (PARTITION BY sl.complaint_id ORDER BY sl.status_date) AS next_status_date
    FROM status_logs sl
) t
WHERE next_status_date IS NOT NULL
GROUP BY status;

-- Query 10 | Categories with the longest average resolution time
WITH complaint_turnaround AS (
    SELECT 
        c.complaint_id,
        c.category_id,
        MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) 
            - MIN(sl.status_date) AS turnaround_days
    FROM complaints c
    JOIN status_logs sl ON c.complaint_id = sl.complaint_id
    GROUP BY c.complaint_id, c.category_id
    HAVING MAX(CASE WHEN sl.status = 'Resolved' THEN sl.status_date END) IS NOT NULL
)
SELECT 
    sc.category_name,
    ROUND(AVG(ct.turnaround_days)) AS avg_days_to_resolve
FROM complaint_turnaround ct
JOIN service_categories sc ON ct.category_id = sc.category_id
GROUP BY sc.category_name
ORDER BY avg_days_to_resolve DESC
LIMIT 5;

-- Add more variance to the categories with the longest average resolution time
