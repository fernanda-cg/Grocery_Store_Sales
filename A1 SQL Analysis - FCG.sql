USE bos_fmban_sql_analysis;

-- REPRESENTATION BY CATEGORY IN % AND AVERAGE PRICES --

-- third CTE to get representation table with total row
WITH representation_table AS (

-- second CTE to obtain final data set without duplicates
WITH final_data AS (

-- first CTE to get rid of duplicates, keeping only one record per ID
WITH duplicates AS (
SELECT 
data_entry_order, ID, category, subcategory, brand, name_of_product, regular_price, sale_price, weight, unit,
alcoholic, dairyfree, vegan, vegetarian, lowsodium, paleofriendly, sugarconscious, whole_foods_diet, ketofriendly, kosher, lowfat,
organic, glutenfree, engine_2, `local`
FROM (SELECT 
    *, row_number() over(partition by ID ORDER BY ID) AS RowNumber
FROM
    bfmban_data 
WHERE
    ID IN (SELECT 
            datab.ID
        FROM
            bfmban_data AS datab
        GROUP BY datab.ID
        HAVING COUNT(datab.ID) > 1
        ORDER BY COUNT(datab.ID) DESC)) AS duplicates
WHERE RowNumber = 1)

-- First select to get only unique values from original table, fixing category discrepancies as well
SELECT 
    *,
    -- Fixing category mistakes, typos and grouping together categories with small sample size (ie seafood and meat combined)
    CASE
        WHEN category LIKE '%BREAD ROLL%' THEN 'BREAD, ROLLS & BAKERY'
        WHEN category LIKE '%FROZEN FOOD%' THEN 'FROZEN FOODS'
        WHEN category LIKE '%SNACKS CHIPS SALSAS%' THEN 'SNACKS, CHIPS, SALSAS & DIPS'
        WHEN category LIKE '%DAIRY%' THEN 'DAIRY & EGGS'
        WHEN category LIKE '%MEAT%' THEN 'MEAT & SEAFOOD'
        WHEN category LIKE '%SEAFOOD%' THEN 'MEAT & SEAFOOD'
        WHEN category LIKE '%WINE%' THEN 'BEVERAGES'
        WHEN category LIKE '%BEAUTY%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%BODY%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%SUPPLEMENTS%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%FLORAL%' THEN 'OTHER'
        WHEN category LIKE '%LIFESTYLE%' THEN 'OTHER'
        ELSE category
    END AS new_cat
FROM
    bfmban_data
WHERE
    ID NOT IN (SELECT 
            datab.ID
        FROM
            bfmban_data AS datab
        GROUP BY datab.ID
        HAVING COUNT(datab.ID) > 1
        ORDER BY COUNT(datab.ID) DESC)
UNION -- Using union to combine both tables
SELECT *,
	-- Fixing category mistakes, typos and grouping together categories with small sample size (ie seafood and meat combined)
    CASE
        WHEN category LIKE '%BREAD ROLL%' THEN 'BREAD, ROLLS & BAKERY'
        WHEN category LIKE '%FROZEN FOOD%' THEN 'FROZEN FOODS'
        WHEN category LIKE '%SNACKS CHIPS SALSAS%' THEN 'SNACKS, CHIPS, SALSAS & DIPS'
        WHEN category LIKE '%DAIRY%' THEN 'DAIRY & EGGS'
        WHEN category LIKE '%MEAT%' THEN 'MEAT & SEAFOOD'
        WHEN category LIKE '%SEAFOOD%' THEN 'MEAT & SEAFOOD'
        WHEN category LIKE '%WINE%' THEN 'BEVERAGES'
        WHEN category LIKE '%BEAUTY%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%BODY%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%SUPPLEMENTS%' THEN 'BEAUTY & BODY CARE'
        WHEN category LIKE '%FLORAL%' THEN 'OTHER'
        WHEN category LIKE '%LIFESTYLE%' THEN 'OTHER'
        ELSE category
    END AS new_cat FROM duplicates -- Combining duplicates with original data considering only unique values to obtain final database
ORDER BY new_cat)

SELECT 
	new_cat AS 'Category',
	COUNT(ID) AS 'Number of products',
    COUNT(CASE  -- Counting # of products that are sold by any of the Whole Foods conglorate. 
			WHEN brand LIKE '%WHOLE%' THEN 1 -- We're counting any brand that contains the word WHOLE as part of WFM, to consider any typing discrepancies
            WHEN brand LIKE '%365%' THEN 1 -- Counting any brand that contains 365 as part of WFM
            WHEN brand LIKE '%ALLEGRO%' THEN 1 -- Counting Allegro as part of WFM
		 END) AS 'Number of products from WFM',
	CONCAT(FORMAT((COUNT(CASE -- Calculate proportion for each category
			WHEN brand LIKE '%WHOLE%' THEN 1
            WHEN brand LIKE '%365%' THEN 1
            WHEN brand LIKE '%ALLEGRO%' THEN 1
		 END)/COUNT(ID))*100,1), '%') AS 'WFM representation',
	FORMAT(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN regular_price
            WHEN brand LIKE '%365%' THEN regular_price
            WHEN brand LIKE '%ALLEGRO%' THEN regular_price
        END),
        2) AS 'WFM average price', -- Average price considering WFM brands only
    FORMAT(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN NULL
            WHEN brand LIKE '%365%' THEN NULL
            WHEN brand LIKE '%ALLEGRO%' THEN NULL
            ELSE regular_price
        END),
        2) AS 'Rest of brands average price', -- Average price all brands except WFM brands
	CONCAT(FORMAT((AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN regular_price
            WHEN brand LIKE '%365%' THEN regular_price
            WHEN brand LIKE '%ALLEGRO%' THEN regular_price
        END)/(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN NULL
            WHEN brand LIKE '%365%' THEN NULL
            WHEN brand LIKE '%ALLEGRO%' THEN NULL
            ELSE regular_price
        END))-1)*100,1), '%') AS 'Price difference WFM vs rest' -- Difference between WFM and the rest of brands for each category
FROM final_data
GROUP BY new_cat
UNION -- Combining with row for TOTAL
SELECT
	CASE WHEN new_cat LIKE '%%' THEN 'TOTAL' END AS new_cat2,
	COUNT(ID) AS 'Number of products',
    COUNT(CASE  -- Counting # of products that are sold by any of the Whole Foods conglorate. 
			WHEN brand LIKE '%WHOLE%' THEN 1 -- We're counting any brand that contains the word WHOLE as part of WFM, to consider any typing discrepancies
            WHEN brand LIKE '%365%' THEN 1 -- Counting any brand that contains 365 as part of WFM
            WHEN brand LIKE '%ALLEGRO%' THEN 1 -- Counting Allegro as part of WFM
		 END) AS 'Number of products from WFM',
	CONCAT(FORMAT((COUNT(CASE -- Calculate proportion for TOTAL
			WHEN brand LIKE '%WHOLE%' THEN 1
            WHEN brand LIKE '%365%' THEN 1
            WHEN brand LIKE '%ALLEGRO%' THEN 1
		 END)/COUNT(ID))*100,1), '%') AS 'WFM representation',
	FORMAT(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN regular_price
            WHEN brand LIKE '%365%' THEN regular_price
            WHEN brand LIKE '%ALLEGRO%' THEN regular_price
        END),
        2) AS 'WFM average price',-- Average price considering WFM brands only
    FORMAT(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN NULL
            WHEN brand LIKE '%365%' THEN NULL
            WHEN brand LIKE '%ALLEGRO%' THEN NULL
            ELSE regular_price
        END),
        2) AS 'Rest of brands average price', -- Average price all brands except WFM brands
	CONCAT(FORMAT((AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN regular_price
            WHEN brand LIKE '%365%' THEN regular_price
            WHEN brand LIKE '%ALLEGRO%' THEN regular_price
        END)/(AVG(CASE
            WHEN brand LIKE '%WHOLE%' THEN NULL
            WHEN brand LIKE '%365%' THEN NULL
            WHEN brand LIKE '%ALLEGRO%' THEN NULL
            ELSE regular_price
        END))-1)*100,1), '%') AS 'Price difference WFM vs rest' -- Difference between WFM and the rest of brands for each category
FROM final_data
GROUP BY new_cat2)

SELECT * FROM representation_table;
