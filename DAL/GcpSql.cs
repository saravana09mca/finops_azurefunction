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
    public class GcptoSql
    {
        public static bool SaveBillingCost(List<GCPBillingCostModel.GCPBillingCost> listGCPDdata, ILogger log)
        {
            bool result = false;
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
                            
                            DeleteGCPBillingcostFromDate(log);
                            log.LogInformation($"SQL Bulk Copy - No of Rows Data: {dt.Rows.Count}");
                            SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                            bcp.DestinationTableName = "GCPBillingData";
                            bcp.BatchSize = 10000;
                            bcp.WriteToServer(dt);
                            log.LogInformation("SQL Bulk Copy Completed");
                        }
                        catch (TransactionException ex)
                        {
                            transactionScope.Dispose();
                            throw new Exception($"Transaction Exception Occured - {ex.Message}");
                        }
                        transactionScope.Complete();
                    }
                }
                result = true;
            }
            catch (Exception ex)
            {
                log.LogError(exception: ex, ex.Message);
            }
            return result;
        }
        public static void DeleteGCPBillingcostFromDate(ILogger log)
        {
            log.LogInformation($"SQL Delete Process Start");
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            int batchSize = 10000;
            using (SqlConnection connection = new SqlConnection(myConnectionString))
            {
                connection.Open();
                // create a SQL command object with the DELETE statement
                using (SqlCommand command = new SqlCommand("DELETE TOP (@BatchSize) GCPBillingData where cast(ExportTime as date)>='" + ConfigStore.GCP_FromDate + "'", connection))
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
                    log.LogInformation($"Deleted {rowsAffected} rows");
                }
                connection.Close();
            }            
          
        }
       
     

        public static bool DeleteGcpTagsExists()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            bool result = false;
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete  GCPResourceTags", con);
                try
                {
                    result = Convert.ToBoolean(objSqlCommand.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    con.Close();
                    throw new Exception(ex.Message, ex);
                }
            }
            return result;
        }
        public static bool SaveGcpTags(List<GcpTagsModel.GcpTags> listGCPTagsdata, ILogger log)
        {
            bool result = false;
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
                    log.LogInformation($"GCP Tags - Start deleting existing records ");
                    DeleteGcpTagsExists();
                    log.LogInformation($"GCP Tags Existing records deleted");
                    log.LogInformation($"GCP Tags SQL Bulk Copy Start - Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPResourceTags";
                    bcp.WriteToServer(dt);
                    log.LogInformation("SQL Bulk Copy Completed");
                }
                result = true;
            }
            catch (Exception ex)
            {
                log.LogError(exception: ex, ex.Message);
            }
            return result;
        }
        public static bool SaveGcpAdvisor(List<GCPAdvisorModel.GCPAdvisor> objAdvisor, ILogger log)
        {
            bool result = false;
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
                    DeleteGcpAdvisorExistsData();
                    log.LogInformation($"Gcp Advisor Delete Exists Data Processed");
                    log.LogInformation($"Gcp Advisor  SQL Bulk Copy Start- Count: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPAdvisorRecommendation";
                    bcp.WriteToServer(dt);
                    log.LogInformation($"Gcp Advisor  SQL Bulk Copy Completed");
                }
                result = true;
            }
            catch (Exception ex)
            {
                log.LogError(exception: ex, ex.Message);
            }
            return result;
        }
        public static bool DeleteGcpAdvisorExistsData()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            bool result = false;
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete GCPAdvisorRecommendation", con);
                try
                {
                    result = Convert.ToBoolean(objSqlCommand.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    con.Close();
                    throw new Exception(ex.Message,ex);
                }
            }
            return result;
        }

    }
}
