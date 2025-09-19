IF OBJECT_ID ('silver.crm_cust_info' , 'U') IS NOT NULL 
DROP TABLE silver.crm_cust_info;


CREATE TABLE silver.crm_cust_info (
cst_id INT ,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.crm_prd_info' , 'U') IS NOT NULL 
DROP TABLE silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
prd_id INT ,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost int,
prd_line NVARCHAR(50),
prd_start_dt datetime,
prd_end_dt  DATETIME,
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('silver.crm_sales_details' , 'U') IS NOT NULL 
DROP TABLE silver.crm_sales_details;

CREATE TABLE silver.crm_sales_details (
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dr int,
sls_ship_dt int,
sls_due_dt int ,
sls_sales int ,
sls_quantity int ,
sls_price int,
dwh_create_date DATETIME2 DEFAULT GETDATE() 
);
GO
IF OBJECT_ID ('silver.erp_loc_a101' , 'U') IS NOT NULL 
DROP TABLE silver.erp_loc_a101;

CREATE TABLE silver.erp_loc_a101 (
cid nvarchar(50),
cntry nvarchar(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_cust_az12' , 'U') IS NOT NULL 
DROP TABLE silver.erp_cust_az12;

CREATE TABLE silver.erp_cust_az12 (
cid nvarchar (50),
bdate date ,
gen nvarchar(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID ('silver.erp_px_cat_g1v2' , 'U') IS NOT NULL 
DROP TABLE silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2 (
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50),
maintenance nvarchar(50),
dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
