using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;



namespace Budget.TimerFunction
{
    public class GcptoSql:IGcpSql
    {
        private  readonly ILogger<GcptoSql> _logger;

        public  GcptoSql(ILogger<GcptoSql> logger)
        {
            _logger = logger;
        }
        public void SaveBillingCost(List<GCPBillingCostModel.GCPBillingCost> listGCPDdata)
        {
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("DataId");
                dt.Columns.Add("BillingAccountId");
                dt.Columns.Add("ProjectId");
                dt.Columns.Add("ProjectNumber");
                dt.Columns.Add("ProjectName");
                dt.Columns.Add("SkuId");
                dt.Columns.Add("SkuDesc");
                dt.Columns.Add("UsageStartDate");
                dt.Columns.Add("Date");
                dt.Columns.Add("ServiceId");
                dt.Columns.Add("ServiceDesc");
                dt.Columns.Add("Location");
                dt.Columns.Add("Region");
                dt.Columns.Add("Cost");
                dt.Columns.Add("CostUsd");
                dt.Columns.Add("ExportTime");
                dt.Columns.Add("Currency");
                dt.Columns.Add("CurrencyConversionRate");
                dt.Columns.Add("ResourceName");
                dt.Columns.Add("ResourceId");
                dt.Columns.Add("DataInsertDate");           

                foreach (GCPBillingCostModel.GCPBillingCost data in listGCPDdata)
                {

                    if (!string.IsNullOrEmpty(data.ResourceName))
                    {
                        data.ResourceName = data.ResourceName.Split('/').Last();
                    }
                    if (!string.IsNullOrEmpty(data.ResourceId))
                    {
                        data.ResourceId = data.ResourceId.Split('/').Last();
                    }
                    string billingDataId= Guid.NewGuid().ToString();

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["DataId"] = billingDataId;
                    row["BillingAccountId"] = data.BillingAccountId;
                    row["ProjectId"] = data.ProjectId;
                    row["ProjectNumber"] = data.ProjectNumber;
                    row["ProjectName"] = data.ProjectName;
                    row["SkuId"] = data.SkuId;
                    row["SkuDesc"] = data.SkuDesc;
                    row["UsageStartDate"] = data.UsageStartDate;
                    row["Date"] = data.Date;
                    row["ServiceId"] = data.ServiceId;
                    row["ServiceDesc"] = data.ServiceDesc;
                    row["Location"] = data.Location;
                    row["Region"] = data.Region;
                    row["Cost"] = Helper.ValidateDecimal(data.Cost.ToString());
                    row["CostUsd"] = data.CostUsd;
                    row["ExportTime"] = data.ExportTime;
                    row["Currency"] = data.Currency;
                    row["CurrencyConversionRate"] = data.CurrencyConversionRate;
                    row["ResourceName"] = data.ResourceName;
                    row["ResourceId"] = data.ResourceId;
                    row["DataInsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }


                if (dt.Rows.Count > 0)
                {
                  
                    
                    using (TransactionScope transactionScope = new TransactionScope())
                    {
                        try
                        {
                            _logger.LogInformation($"GCP Billing Cost - Delete process start");
                            DeleteGCPBillingcostFromDate();
                            _logger.LogInformation($"Gcp Billing Cost - SQL Bulk Copy - No of Rows Data: {dt.Rows.Count}");
                            SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                            bcp.DestinationTableName = "GCPBillingData";
                            bcp.BatchSize = 10000;
                            bcp.WriteToServer(dt);
                            _logger.LogInformation("Gcp Billing Cost  -SQL Bulk Copy Completed");
                        }
                        catch (TransactionException ex)
                        {
                            transactionScope.Dispose();
                            throw new Exception($"Gcp Billing Cost - Transaction Exception Occured - {ex.Message}");
                        }
                        transactionScope.Complete();
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
        }
        public void DeleteGCPBillingcostFromDate()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            int batchSize = 10000;
            using (SqlConnection connection = new SqlConnection(myConnectionString))
            {
                connection.Open();
                // create a SQL command object with the DELETE statement
                using (SqlCommand command = new SqlCommand("DELETE TOP (@BatchSize) GCPBillingData where cast(ExportTime as date)>='" + ConfigStore.GCP.GCP_FromDate + "'", connection))
                {

                    // add parameter to the command
                    command.Parameters.AddWithValue("@BatchSize", batchSize);

                    // execute the command to delete the records in batches
                    int rowsAffected = 0;
                    while (true)
                    {
                        int batchRowsAffected = command.ExecuteNonQuery();
                        if (batchRowsAffected == 0)
                        {
                            break;
                        }
                        rowsAffected += batchRowsAffected;
                    }
                    _logger.LogInformation($"GCP BillingCost Deleted {rowsAffected} rows");
                }
                connection.Close();
            }            
          
        }
       
     

        public void DeleteGcpTagsExists()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
        
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPResourceTags", con);
                try
                {
                    int rows = objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Tags rows {rows} deleted");
                }
                catch (Exception ex)
                {
                    con.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
        }
        public void SaveGcpTags(List<GcpTagsModel.GcpTags> listGCPTagsdata)
        {
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("ProjectId");
                dt.Columns.Add("ServiceId");
                dt.Columns.Add("ServiceDesc");
                dt.Columns.Add("ResourceId");
                dt.Columns.Add("Key");
                dt.Columns.Add("Value");

                foreach (GcpTagsModel.GcpTags data in listGCPTagsdata)
                {
                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["ProjectId"] = data.ProjectId;
                    row["ServiceId"] = data.ServiceId;
                    row["ServiceDesc"] = data.ServiceDesc;
                    row["ResourceId"] = (!string.IsNullOrEmpty(data.ResourceId))?data.ResourceId.Split('/').Last(): data.ResourceId;
                    row["Key"] = data.TagKey;
                    row["Value"] = data.TagValue;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    _logger.LogInformation($"GCP Tags - Deleting process start. ");
                    DeleteGcpTagsExists();
                    _logger.LogInformation($"GCP Tags - SQL Bulk Copy Start - Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPResourceTags";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation("GCP Tags - SQL Bulk Copy Completed");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(exception: ex, ex.Message);
            }
        }
        public void SaveGcpAdvisor(List<GCPAdvisorModel.GCPAdvisor> objAdvisor)
        {
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("ProjectNumber");
                dt.Columns.Add("Name");
                dt.Columns.Add("Location");
                dt.Columns.Add("Type");
                dt.Columns.Add("Subtype");
                dt.Columns.Add("Description");
                dt.Columns.Add("Category");
                dt.Columns.Add("Units");
                dt.Columns.Add("Nanos");
                dt.Columns.Add("CostInUSD");
                dt.Columns.Add("Severity");
                dt.Columns.Add("LastRefreshTime");
                dt.Columns.Add("InsertDate");

                foreach (GCPAdvisorModel.GCPAdvisor data in objAdvisor)
                {   
                    double? cost = (((data.Units == null) ? 0 : data.Units) + (data.Nanos / Math.Pow(10, 9)));

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["ProjectNumber"] = data.ProjectNumber;
                    row["Name"] = data.Name;
                    row["Location"] = data.Location;
                    row["Type"] = data.Type;
                    row["Subtype"] = data.SubType;
                    row["Description"] = data.Description;
                    row["Category"] = data.Category;
                    row["Units"] = (data.Units == null)?0: data.Units;
                    row["Nanos"] = data.Nanos;
                    row["CostInUSD"] = (cost != null)?cost:0.00;
                    row["Severity"] = data.Severity;
                    row["LastRefreshTime"] = data.LastRefreshDate;
                    row["InsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    _logger.LogInformation($"Gcp Advisor - Delete process start");
                    DeleteGcpAdvisorExistsData();
                    _logger.LogInformation($"Gcp Advisor - SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPAdvisorRecommendation";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation($"Gcp Advisor  SQL Bulk Copy Completed");
                }
            }
            catch (Exception)
            {
                throw;
            }
          
        }
        public void DeleteGcpAdvisorExistsData()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPAdvisorRecommendation", con);
                try
                {
                    int rows =  objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Advisor rows {rows} deleted");
                }
                catch (Exception)
                {
                    con.Close();
                    throw;
                }
            }
        }
        public void SaveGcpUtilization(List<GCPUtilizationModel.GCPUtilization> objUtilization, string date)
        {
         
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("MetricName");
                dt.Columns.Add("ProjectId");
                dt.Columns.Add("InstanceId");
                dt.Columns.Add("Date");
                dt.Columns.Add("AverageUtilization");
                dt.Columns.Add("MinmumUtilization");
                dt.Columns.Add("MaximumUtilization");
                dt.Columns.Add("InsertDate");

                foreach (GCPUtilizationModel.GCPUtilization data in objUtilization)
                {
                   

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["MetricName"] = data.MetricName;
                    row["ProjectId"] = data.ProjectId;
                    row["InstanceId"] = data.InstanceId;
                    row["Date"] = data.Date;
                    row["AverageUtilization"] = data.AvgUtilization;
                    row["MinmumUtilization"] = data.MinUtilization;
                    row["MaximumUtilization"] = data.MaxUtilization;
                    row["InsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    _logger.LogInformation($"Gcp Utilization - Delete Process start");
                    DeleteGcpUtilizationExistsData(date);                    
                    _logger.LogInformation($"Gcp Utilization - SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPUtilization";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation($"Gcp Utilization - SQL Bulk Copy Completed");
                }
                
            }
            catch (Exception)
            {
                throw;
            }
            
        }
     
        public void DeleteGcpUtilizationExistsData(string date)
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
         
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPUtilization where [date] ='" + date+"'", con);
                try
                {
                    int rows = objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Utilization rows {rows} deleted");
                }
                catch (Exception)
                {
                    con.Close();
                    throw;
                }
            }
        }
        public void SaveGcpBudget(List<GcpBudgetModel.GcpBudget> objBudgetList, string date)
        {
         
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("BudgetName");
                dt.Columns.Add("Date");
                dt.Columns.Add("BudgetAmount");
                dt.Columns.Add("Cost");
                dt.Columns.Add("BudgetAmountType");
                dt.Columns.Add("CurrencyCode");
                dt.Columns.Add("InsertDate");

                foreach (GcpBudgetModel.GcpBudget data in objBudgetList)
                {
                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["BudgetName"] = data.budgetDisplayName;
                    row["Date"] = data.costIntervalStart;
                    row["BudgetAmount"] = data.budgetAmount;
                    row["Cost"] = data.costAmount;
                    row["BudgetAmountType"] = data.budgetAmountType;
                    row["CurrencyCode"] = data.currencyCode;
                    row["InsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    _logger.LogInformation($"Gcp Budget - Deleting Process start.");
                    DeleteGcpBudgetData(date);
                    
                    _logger.LogInformation($"Gcp Budget - SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPBudgetData";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation($"Gcp Budget - SQL Bulk Copy Completed");
                }
                
            }
            catch (Exception)
            {
                throw;
            }
            
        }
        public void DeleteGcpBudgetData(string date)
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
         
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPBudgetData where cast(Date as date)>='"+date+"'", con);
                try
                {
                    int rows = objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Budget rows {rows} deleted");
                }
                catch (Exception)
                {
                    con.Close();
                    throw;
                }
            }
            
        }
        public void SaveGcpOrphaned(List<GCPAdvisorModel.GCPAdvisor> objAdvisor)
        {
         
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("ProjectNumber");
                dt.Columns.Add("Name");
                dt.Columns.Add("Location");
                dt.Columns.Add("Type");
                dt.Columns.Add("Description");
                dt.Columns.Add("Category");
                dt.Columns.Add("LastRefreshTime");
                dt.Columns.Add("InsertDate");

                foreach (GCPAdvisorModel.GCPAdvisor data in objAdvisor)
                {
                    double? cost = (((data.Units == null) ? 0 : data.Units) + (data.Nanos / Math.Pow(10, 9)));

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["ProjectNumber"] = data.ProjectNumber;
                    row["Name"] = data.Name;
                    row["Location"] = data.Location;
                    row["Type"] = data.Type;
                    row["Description"] = data.Description;
                    row["Category"] = data.Category;
                    row["LastRefreshTime"] = data.LastRefreshDate;
                    row["InsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    _logger.LogInformation($"Gcp Orphaned - Delete Process start.");
                    DeleteGcpOrphanedExistsData();
                    _logger.LogInformation($"Gcp Orphaned - SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPOrhpanedData";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation($"Gcp Orphaned - SQL Bulk Copy Completed");
                }
                
            }
            catch (Exception)
            {
                throw;
            }
            
        }
        public void DeleteGcpOrphanedExistsData()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
         
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPOrhpanedData", con);
                try
                {
                    int rows = objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Orphaned rows {rows} deleted");
                }
                catch (Exception)
                {
                    con.Close();
                    throw;
                }
            }
            
        }

        public void SaveCarbonFootPrint(DataTable dt, string date)
        {
         
            try
            {
                var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");              

                if (dt.Rows.Count > 0)
                {

                    _logger.LogInformation($"Gcp Carbon FootPrint - Delete Process start.");
                    DeleteGcpCarbonExistsData(date);
                    _logger.LogInformation($"Gcp Carbon FootPrint - SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPCarbonFootPrint";
                    bcp.WriteToServer(dt);
                    _logger.LogInformation($"Gcp Carbon FootPrint - SQL Bulk Copy Completed");
                }
                
            }
            catch (Exception ex)
            {
                throw;
            }
            
        }
        public void DeleteGcpCarbonExistsData(string date)
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
         
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPCarbonFootPrint where cast(UsageMonth as date)>='" + date + "'", con);
                try
                {
                    int rows = objSqlCommand.ExecuteNonQuery();
                    _logger.LogInformation($"Gcp Carbon FootPrint rows {rows} deleted");
                }
                catch (Exception)
                {
                    con.Close();
                    throw;
                }
            }            
        }
    }
}
