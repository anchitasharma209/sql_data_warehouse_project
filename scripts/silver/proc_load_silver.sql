 -- EXEC silver.load_silver
   
   CREATE OR ALTER PROCEDURE silver.load_silver AS 
    BEGIN

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_cust_info';
        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Inserting Data Into: silver.crm_cust_info'
        INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
        select cst_id,cst_key,
        trim(cst_firstname) as cst_firstname,
        trim(cst_lastname) as cst_lastname,
        CASE WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
             WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
             ELSE 'n/a'
        END
        cst_marital_status,
        CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
             ELSE 'n/a'
        END
        cst_gndr,
        cst_create_date
        from (
        select * ,
        ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date desc) as flag_last
        FROM bronze.crm_cust_info
        where cst_id is not null)t 
        where flag_last = 1 ;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        SET @start_time= GETDATE()
        PRINT '>> Truncating Table: silver.crm_prd_info';
        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Inserting Data Into: silver.crm_prd_info';
        INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

        Select prd_id,

        REPLACE(SUBSTRING(prd_key , 1 , 5), '-' , '_') as cat_id,
        SUBSTRING(prd_key , 7 , LEN(prd_key)) as prd_key,
        prd_nm,
        ISNULL(prd_cost , 0) as prd_cost,
        CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
             WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
             WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
             WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
        END as prd_line,

        Cast(prd_start_dt as DATE) as prd_start_dt,
        Cast(lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) -1 as date) as prd_end_dt

        from bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';




        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.crm_sales_details';
        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Inserting Data Into: silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key ,
        sls_cust_id ,
        sls_order_dt ,
        sls_ship_dt,
        sls_due_dt  ,
        sls_sales  ,
        sls_quantity  ,
        sls_price 

        )

        SELECT sls_ord_num,
              sls_prd_key,
              sls_cust_id,
              CASE WHEN sls_order_dt = 0 or len(sls_order_dt) != 8 THEN NULL
              ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
              END AS sls_order_dt,
              CASE WHEN sls_ship_dt = 0 or len(sls_ship_dt) != 8 THEN NULL
              ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
              END AS sls_ship_dt,
              CASE WHEN sls_due_dt = 0 or len(sls_due_dt) != 8 THEN NULL
              ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
              END AS sls_due_dt,
        CASE WHEN sls_sales is null  or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price) THEN 
             sls_quantity * ABS(sls_price) 
             ELSE sls_sales
             END as sls_sales,
             sls_quantity,
        CASE WHEN sls_price is null  or sls_price <= 0 
             THEN sls_sales / NULLIF(sls_quantity,0)
             ELSE sls_price
             END as sls_price   
         FROM bronze.crm_sales_details;
         SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';





        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_cust_az12';
        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Inserting Data Into: silver.erp_cust_az12'

        INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

        select 
        CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4 , LEN(cid))
             ELSE cid 
        END as cid,

        CASE WHEN bdate > GETDATE() THEN NULL 
             ELSE bdate 
        END as bdate,
        CASE WHEN UPPER(TRIM(gen)) IN ('F' , 'FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(gen)) IN ('M' , 'MALE') THEN 'Male'
             ELSE 'n/a' 
        END as gen

        from bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';



        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_loc_a101';
        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Inserting Data Into: silver.erp_loc_a101';
        INSERT INTO silver.erp_loc_a101(cid,cntry)

        select 
        REPLACE(cid,'-' , '') as cid,
        CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
             WHEN TRIM(cntry) in ('US' , 'USA') THEN 'United States'
             WHEN TRIM(cntry) = '' or cntry IS NULL THEN 'n/a'
        ELSE cntry 
        END as
        cntry
        from bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        SET @start_time = GETDATE()
        PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)

        select 
        id,
        cat,
        subcat,
        maintenance
        from
        bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

 SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

  END TRY
  BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
    END
