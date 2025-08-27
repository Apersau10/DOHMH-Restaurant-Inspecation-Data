;WITH restaurant_data AS (
    SELECT 
        CAMIS,
        CAST(DBA AS VARCHAR(MAX)) AS DBA,
        BORO,
        ZIPCODE,
        Latitude,
        Longitude,
        CAST(CUISINE_DESCRIPTION AS VARCHAR(MAX)) AS CUISINE_DESCRIPTION
    FROM proj1_restaurant_1
    WHERE DBA IS NOT NULL
      AND CUISINE_DESCRIPTION IS NOT NULL
      AND ZIPCODE IS NOT NULL
      AND Latitude IS NOT NULL
      AND Longitude IS NOT NULL
      AND Latitude <> 0
      AND Longitude <> 0
    UNION ALL
    SELECT 
        CAMIS,
        CAST(DBA AS VARCHAR(MAX)) AS DBA,
        BORO,
        ZIPCODE,
        Latitude,
        Longitude,
        CAST(CUISINE_DESCRIPTION AS VARCHAR(MAX)) AS CUISINE_DESCRIPTION
    FROM proj1_restaurant_2
    WHERE DBA IS NOT NULL
      AND CUISINE_DESCRIPTION IS NOT NULL
      AND ZIPCODE IS NOT NULL
      AND Latitude IS NOT NULL
      AND Longitude IS NOT NULL
      AND Latitude <> 0
      AND Longitude <> 0
    UNION ALL
    SELECT 
        CAMIS,
        CAST(DBA AS VARCHAR(MAX)) AS DBA,
        BORO,
        ZIPCODE,
        Latitude,
        Longitude,
        CAST(CUISINE_DESCRIPTION AS VARCHAR(MAX)) AS CUISINE_DESCRIPTION
    FROM proj1_restaurant_3
    WHERE DBA IS NOT NULL
      AND CUISINE_DESCRIPTION IS NOT NULL
      AND ZIPCODE IS NOT NULL
      AND Latitude IS NOT NULL
      AND Longitude IS NOT NULL
      AND Latitude <> 0
      AND Longitude <> 0
),
violation_data AS (
    SELECT 
        CAMIS,
        INSPECTION_DATE,
        CAST(INSPECTION_TYPE AS VARCHAR(MAX)) AS INSPECTION_TYPE,
        CAST(VIOLATION_CODE AS VARCHAR(MAX)) AS VIOLATION_CODE,
        CAST(VIOLATION_DESCRIPTION AS VARCHAR(MAX)) AS VIOLATION_DESCRIPTION,
        CAST(CRITICAL_FLAG AS VARCHAR(MAX)) AS CRITICAL_FLAG,
        SCORE,
        GRADE
    FROM proj1_violation_1
    WHERE VIOLATION_CODE IS NOT NULL
      AND SCORE IS NOT NULL
      AND SCORE >= 0
      AND GRADE IS NOT NULL
    UNION ALL
    SELECT 
        CAMIS,
        INSPECTION_DATE,
        CAST(INSPECTION_TYPE AS VARCHAR(MAX)) AS INSPECTION_TYPE,
        CAST(VIOLATION_CODE AS VARCHAR(MAX)) AS VIOLATION_CODE,
        CAST(VIOLATION_DESCRIPTION AS VARCHAR(MAX)) AS VIOLATION_DESCRIPTION,
        CAST(CRITICAL_FLAG AS VARCHAR(MAX)) AS CRITICAL_FLAG,
        SCORE,
        GRADE
    FROM proj1_violation_2
    WHERE VIOLATION_CODE IS NOT NULL
      AND SCORE IS NOT NULL
      AND SCORE >= 0
      AND GRADE IS NOT NULL
    UNION ALL
    SELECT 
        CAMIS,
        INSPECTION_DATE,
        CAST(INSPECTION_TYPE AS VARCHAR(MAX)) AS INSPECTION_TYPE,
        CAST(VIOLATION_CODE AS VARCHAR(MAX)) AS VIOLATION_CODE,
        CAST(VIOLATION_DESCRIPTION AS VARCHAR(MAX)) AS VIOLATION_DESCRIPTION,
        CAST(CRITICAL_FLAG AS VARCHAR(MAX)) AS CRITICAL_FLAG,
        SCORE,
        GRADE
    FROM proj1_violation_3
    WHERE VIOLATION_CODE IS NOT NULL
      AND SCORE IS NOT NULL
      AND SCORE >= 0
      AND GRADE IS NOT NULL
),
joined_data AS (
    SELECT 
        r.CAMIS,
        r.DBA,
        r.BORO,
        r.ZIPCODE,
        r.Latitude,
        r.Longitude,
        r.CUISINE_DESCRIPTION,
        v.INSPECTION_DATE,
        v.INSPECTION_TYPE,
        v.SCORE,
        v.GRADE,
        v.VIOLATION_CODE,
        v.VIOLATION_DESCRIPTION,
        v.CRITICAL_FLAG
    FROM restaurant_data r
    LEFT JOIN violation_data v 
        ON r.CAMIS = v.CAMIS
)

-- Final Result: 1 Row Per CAMIS with Clean Data
SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY CAMIS ORDER BY CRITICAL_FLAG DESC, SCORE DESC) AS rn
    FROM joined_data
) final
WHERE rn = 1;
