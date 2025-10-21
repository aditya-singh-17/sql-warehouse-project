/* Added load_bronze stored procedure — automates data ingestion into the Bronze layer by 
truncating existing tables and performing bulk inserts from multiple CSV sources (CRM and ERP).
Includes runtime tracking, row count validation, and error handling for robust data loading in the Data Warehouse.
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze as begin
DECLARE @start_time DATETIME,@end_time DATETIME;
begin try

set @start_time=GETDATE();
truncate table bronze.crom_cust_info;
bulk insert bronze.crom_cust_info
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock

);
set @end_time=GETDATE();
print '>>load_duration'+cast(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
select count(*)from bronze.crom_cust_info;



truncate table bronze.crom_prd_info;
bulk insert bronze.crom_prd_info
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock

);
select count(*)from bronze.crom_prd_info;

truncate table bronze.crom_sales_details_id;
bulk insert bronze.crom_sales_details_id
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock

);
select count(*)from bronze.crom_sales_details_id;

truncate table bronze.erp_CUST_AZ12;
bulk insert bronze.erp_CUST_AZ12
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock

);
select count(*)from bronze.erp_CUST_AZ12;

truncate table bronze.erp_LOC_A101;
bulk insert bronze.erp_LOC_A101
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock

);
select count(*)from bronze.erp_LOC_A101;

truncate table bronze.erp_PX_CAT_G1V2;
bulk insert bronze.erp_PX_CAT_G1V2
from 'C:\Users\ADITYA SINGH\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
with  (
firstrow=2,
fieldterminator=',',
tablock
  );
select count(*)from bronze.erp_PX_CAT_G1V2;
end try 
begin catch
    print '========================================='
	print 'Error occured '
	print 'error message'+ERROR_MESSAGE();
	print 'error message'+CAST(ERROR_NUMBER() AS NVARCHAR);
	print 'error message'+CAST(ERROR_STATE() AS NVARCHAR);
	print '========================================='

end catch 
end

