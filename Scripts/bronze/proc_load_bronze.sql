/*

Stored Procedure: Load Bronze Layer (Source -> Bronze)

Script Purpose:
This stored procedure loads data into the 'bronze' schema from external CSV files.
It performs the following actions:
- Truncates the bronze tables before loading data.
- Uses the `BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:

None.

This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC bronze. load_bronze;

*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 

DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; --Declare variables

	BEGIN TRY   -- BEGIN OF  TRY AND CATCH


	SET @batch_start_time = GETDATE();

		PRINT'==========================================================================';
		PRINT'LOADING BRONZ LAYER';
		PRINT'==========================================================================';

		PRINT'--------------------------------------------------------------------------';


		PRINT'==========================================================================';
		PRINT'LOADING CRM TABLES';
		PRINT'==========================================================================';


		SET @start_time = GETDATE(); -------------


		PRINT'>> TRUNCATING TABLE: bronze.crm_cust_info';

		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT'>> Inserting Data Into:bronze.crm_cust_info';



		BULK INSERT 
		bronze.crm_cust_info
		FROM 
		'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE();----------------------

		PRINT'>> LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'

		PRINT'--------------------------------------------------------------------------';



		SET @start_time = GETDATE();



		PRINT'>> TRUNCATING TABLE: bronze.crm_prd_info';

		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT'>> Inserting Data Into:bronze.crm_prd_info';


		BULK INSERT bronze.crm_prd_info
		FROM 'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);


		SET @end_time = GETDATE();

		PRINT'>>LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';




		





		PRINT'>> TRUNCATING TABLE: bronze.crm_sales_details';

		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT'>> Inserting Data Into:bronze.crm_sales_details';



		SET @start_time = GETDATE();

		BULK INSERT bronze.crm_sales_details
		FROM 'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT'>>LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';






		PRINT'>> TRUNCATING TABLE: bronze.erp_loc_a101';

		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT'>> Inserting Data Into:bronze.erp_loc_a101';



		SET @start_time = GETDATE();

		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		

		SET @end_time = GETDATE();

		PRINT'>>LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';



		PRINT'>> TRUNCATING TABLE: bronze.erp_cust_az12';

		TRUNCATE TABLE bronze.erp_cust_az12;	
	
		PRINT'>> Inserting Data Into:bronze.erp_cust_az12';



		SET @start_time = GETDATE();
			
		BULK INSERT bronze.erp_cust_az12
		FROM 'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		

		SET @end_time = GETDATE();

		PRINT'>>LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';


	


		PRINT'>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;	
	
		PRINT'>> Inserting Data Into:bronze.erp_px_cat_g1v2';


		SET @start_time = GETDATE();


		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\004.SQL UDEMY\SQL BARAA\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		SET @end_time = GETDATE();

		PRINT'>>LOAD DURATION: ' 
		+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';






		SET @batch_end_time = GETDATE();
		PRINT'>>LOADING BRONZE LAYER IS COMPLETED';
		PRINT'>>TOTAL LOAD DURATION : ' 
		+ CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds'
		PRINT'--------------------------------------------------------------------------';







	END TRY	 --END OF TRY AND CATCH
		BEGIN CATCH


		-- ADDING SOME INFORMATION IF ANY ERROR HAPPENED


		PRINT'==========================================================================';
		PRINT'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT'Error Message' + ERROR_MESSAGE();
		PRINT'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'==========================================================================';

		END CATCH
END

