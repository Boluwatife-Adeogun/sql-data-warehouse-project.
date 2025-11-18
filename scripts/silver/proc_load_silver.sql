/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
    
		Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
Usage Example:
    CALL Silver.load_silver;
===============================================================================
*/

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    step_start TIMESTAMP;
    duration INTERVAL;
BEGIN
    start_time := clock_timestamp();
    RAISE NOTICE '>> Starting Silver load process at %', start_time;

    -------------------------------------------------------------------------
    -- STEP 1: Load silver.crm_cust_info
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 1: Truncating silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE '>> Step 1: Inserting into silver.crm_cust_info';
    BEGIN
        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 1: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 1 completed in %', duration;


    -------------------------------------------------------------------------
    -- STEP 2: Load silver.crm_prd_info
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 2: Truncating silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE '>> Step 2: Inserting into silver.crm_prd_info';
    BEGIN
        INSERT INTO silver.crm_prd_info (
            prd_id, 
            cat_id, 
            prd_key,
            prd_nm,
            prd_cost,
            prd_line, 
            prd_start_date,
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key FROM 7) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0)::INT AS prd_cost,
            CASE 
                WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            CAST(prd_start_dt AS DATE) AS prd_start_dt,
            CAST(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day'
                AS DATE
            ) AS prd_end_dt
        FROM bronze.crm_prd_info
        WHERE SUBSTRING(prd_key FROM 7) NOT IN (
            SELECT sls_prd_key FROM bronze.crm_sales_details
        );
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 2: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 2 completed in %', duration;


    -------------------------------------------------------------------------
    -- STEP 3: Load silver.crm_sales_details
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 3: Truncating silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE '>> Step 3: Inserting into silver.crm_sales_details';
    BEGIN
        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt, 
            sls_ship_dt,
            sls_due_dt,
            sls_sales, 
            sls_quantity,
            sls_price 
        )
        SELECT 
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) <> 8 THEN NULL
                 ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
            END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) <> 8 THEN NULL
                 ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
            END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) <> 8 THEN NULL
                 ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
            END AS sls_due_dt,
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 
                     OR sls_sales != sls_quantity * ABS(sls_price)
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 3: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 3 completed in %', duration;


    -------------------------------------------------------------------------
    -- STEP 4: Load silver.erp_cust_az12
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 4: Truncating silver.erp_cust_az12';
    TRUNCATE TABLE silver.erp_cust_az12;

    RAISE NOTICE '>> Step 4: Inserting into silver.erp_cust_az12';
    BEGIN
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
                 ELSE cid
            END AS cid,
            CASE WHEN bdate > CURRENT_DATE THEN NULL
                 ELSE bdate
            END AS bdate,
            CASE 
                WHEN gen IS NULL THEN 'n/a'
                WHEN gen ILIKE 'Female' THEN 'Female'
                WHEN gen ILIKE 'Male' THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 4: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 4 completed in %', duration;


    -------------------------------------------------------------------------
    -- STEP 5: Load silver.erp_loc_a101
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 5: Truncating silver.erp_loc_a101';
    TRUNCATE TABLE silver.erp_loc_a101;

    RAISE NOTICE '>> Step 5: Inserting into silver.erp_loc_a101';
    BEGIN
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE 
                WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
                WHEN UPPER(TRIM(cntry)) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE TRIM(cntry)
            END AS cntry
        FROM bronze.erp_loc_a101;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 5: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 5 completed in %', duration;


    -------------------------------------------------------------------------
    -- STEP 6: Load silver.erp_px_cat_g1v2
    -------------------------------------------------------------------------
    step_start := clock_timestamp();
    RAISE NOTICE '>> Step 6: Truncating silver.erp_px_cat_g1v2';
    TRUNCATE TABLE silver.erp_px_cat_g1v2;

    RAISE NOTICE '>> Step 6: Inserting into silver.erp_px_cat_g1v2';
    BEGIN
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            cat,
            subcat,
            maintenance
        FROM bronze.erp_px_cat_g1v2;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'Error in Step 6: %', SQLERRM;
    END;

    duration := clock_timestamp() - step_start;
    RAISE NOTICE '>> Step 6 completed in %', duration;


    -------------------------------------------------------------------------
    -- FINAL COMPLETION TIME
    -------------------------------------------------------------------------
    duration := clock_timestamp() - start_time;
    RAISE NOTICE '>> Silver load process completed in %', duration;

END;
$$;
