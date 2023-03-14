using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;


namespace Budget.TimerFunction
{
    public class GcptoSql
    {
        public static bool SaveBillingCost(List<GCPBillingModel> listGCPDdata, ILogger log)
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

                foreach (GCPBillingModel data in listGCPDdata)
                {

                    if (!string.IsNullOrEmpty(data.resource.name))
                    {
                        data.resource.name = data.resource.name.Split('/').Last();
                    }
                    if (!string.IsNullOrEmpty(data.resource.global_name))
                    {
                        data.resource.global_name = data.resource.global_name.Split('/').Last();
                    }
                    string billingDataId= Guid.NewGuid().ToString();

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["DataId"] = billingDataId;
                    row["BillingAccountId"] = data.billing_account_id;
                    row["ProjectId"] = data.project.id;
                    row["ProjectNumber"] = data.project.number;
                    row["ProjectName"] = data.project.id;
                    row["SkuId"] = data.sku.id;
                    row["SkuDesc"] = data.sku.description;
                    row["UsageStartDate"] = data.usage_start_time;
                    row["Date"] = data.usage_end_time;
                    row["ServiceId"] = data.service.id;
                    row["ServiceDesc"] = data.service.description;
                    row["Location"] = data.location.location;
                    row["Region"] = data.location.region;
                    row["Cost"] = Helper.ValidateDecimal(data.cost.ToString());
                    row["CostUsd"] = data.CostUsd;
                    row["ExportTime"] = data.export_time;
                    row["Currency"] = data.currency;
                    row["CurrencyConversionRate"] = data.currency_conversion_rate;
                    row["ResourceName"] = data.resource.name;
                    row["ResourceId"] = data.resource.global_name;
                    row["DataInsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }


                if (dt.Rows.Count > 0)
                {
                    log.LogInformation($"SQL Bulk Copy - No of Rows Data: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPBillingData";
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
        public static bool CheckBillingCostDateExists(string fromDate, String toDate)
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            bool result = false;
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("select 1 from GCPBillingData where  CAST(ExportTime AS DATE) between '" + fromDate + "' and '" + toDate + "'", con);
                try
                {
                    result = Convert.ToBoolean(objSqlCommand.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    con.Close();
                }
            }
            return result;
        }

        public static bool DeleteGcpTagsExists()
        {
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            bool result = false;
            using (SqlConnection con = new SqlConnection(myConnectionString))
            {
                con.Open();
                SqlCommand objSqlCommand = new SqlCommand("delete  GCPBillingDataTags", con);
                try
                {
                    result = Convert.ToBoolean(objSqlCommand.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    con.Close();
                }
            }
            return result;
        }
        public static bool SaveGcpTags(List<GcpTagsModel> listGCPTagsdata, ILogger log)
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

                foreach (GcpTagsModel data in listGCPTagsdata)
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
                    log.LogInformation($"SQL Bulk Copy - No of Rows Data: {dt.Rows.Count}");
                    DeleteGcpTagsExists();
                    log.LogInformation($"Existing records deleted");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPBillingDataTags";
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
        public static bool SaveGcpAdvisor(List<GCPAdvisorModel> listGCPAdvisordata, ILogger log)
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
                dt.Columns.Add("Recommender");
                dt.Columns.Add("RecommenderSubtype");
                dt.Columns.Add("Description");
                dt.Columns.Add("Category");
                dt.Columns.Add("Units");
                dt.Columns.Add("Nanos");
                dt.Columns.Add("CostInUSD");
                dt.Columns.Add("Priority");
                dt.Columns.Add("LastRefreshTime");
                dt.Columns.Add("InsertDate");

                foreach (GCPAdvisorModel data in listGCPAdvisordata)
                {
                    //decimal costInNanodollars = Convert.ToDecimal(data.primary_impact.cost_projection.cost.units * 1e9M + data.primary_impact.cost_projection.cost.nanos);
                    //decimal costInUSD = costInNanodollars / 1e9M;
                    double? cost = (data.primary_impact.cost_projection.cost.units + (data.primary_impact.cost_projection.cost.nanos / Math.Pow(10, 9)));

                    DataRow row = dt.NewRow();
                    row["Id"] = null;
                    row["ProjectNumber"] = data.cloud_entity_id;
                    row["Name"] = data.name;
                    row["Location"] = data.location;
                    row["Recommender"] = data.recommender;
                    row["RecommenderSubtype"] = data.recommender_subtype;
                    row["Description"] = data.description;
                    row["Category"] = data.primary_impact.category;
                    row["Units"] = data.primary_impact.cost_projection.cost.units;
                    row["Nanos"] = data.primary_impact.cost_projection.cost.nanos;
                    row["CostInUSD"] = (cost != null)?cost:0.00;
                    row["Priority"] = data.priority;
                    row["LastRefreshTime"] = data.last_refresh_time;
                    row["InsertDate"] = DateTime.UtcNow;
                    dt.Rows.Add(row);
                }

                if (dt.Rows.Count > 0)
                {
                    DeleteGcpAdvisorExistsData();
                    log.LogInformation($"DeleteGcpAdvisorExistsData Processed");
                    log.LogInformation($"SQL Bulk Copy for advisor - No of Rows Data: {dt.Rows.Count}");
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "GCPAdvisorRecommendation";
                    bcp.WriteToServer(dt);
                    log.LogInformation("SQL Bulk Copy for advisor completed");
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
                }
            }
            return result;
        }

    }
}
