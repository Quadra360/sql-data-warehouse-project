
EXEC bronze.load_bronze

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS	BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		
		SET @batch_start_time=GETDATE()
        PRINT '=================================================================================================';
		PRINT 'LOADING BRONZE TABLE';
		PRINT '=================================================================================================';


		PRINT '=================================================================================================';
		PRINT 'LOADING CRM TABLE';
		PRINT '=================================================================================================';

		SET @start_time=GETDATE()
		PRINT'>> TRUCATING TABLE: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info
		PRINT'>> INSERTING INTO TABLE : bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE()

		PRINT 'Time taken for load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);

		PRINT CHAR(13) + CHAR(10);


		SET @start_time=GETDATE()
		PRINT'>> TRUCATING TABLE: bronze.crm_prd_info'
		TRUNCATE TABLE bronze.crm_prd_info
		PRINT'>> INSERTING INTO TABLE : bronze.crm_prd_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE()
		PRINT 'Time taken for load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);

		SET @start_time=GETDATE()

		PRINT'>> TRUCATING TABLE: bronze.crm_sales_details'
		TRUNCATE TABLE bronze.crm_sales_details
		PRINT'>> INSERTING INTO TABLE : bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @end_time=GETDATE()
		PRINT 'Time taken for load: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR);

		PRINT '=================================================================================================';
		PRINT 'LOADING CRM TABLE COMPELTED';
		PRINT '=================================================================================================';


		PRINT '=================================================================================================';
		PRINT 'LOADING ERP TABLE';
		PRINT '=================================================================================================';

		--DATA LOAD FOR ERP SOURCE

		PRINT'>> TRUCATING TABLE: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_cust_az12
		PRINT'>> INSERTING INTO TABLE : bronze.erp_cust_az12'
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		PRINT'>> TRUCATING TABLE: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101
		PRINT'>> INSERTING INTO TABLE : bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		PRINT'>> TRUCATING TABLE: bronze.erp_px_cat_g1v2'
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		PRINT'>> INSERTING INTO TABLE : bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\SQL\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);

		SET @batch_end_time= GETDATE()

		PRINT '=================================================================================================';
		PRINT 'LOADING ERP TABLE COMPLETED';
		PRINT '=================================================================================================';

		PRINT '=================================================================================================';
		PRINT 'LOADING BRONZE TABLE COMPLETED';
		PRINT '=================================================================================================';

		PRINT'>>>> Time taken for entire load ' + CAST(DATEDIFF(second, @batch_start_time,@batch_end_time) AS NVARCHAR) +' seconds';

	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED WHILE LAODING';
	END CATCH

END
