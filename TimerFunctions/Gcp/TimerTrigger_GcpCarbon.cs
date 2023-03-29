using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Google.Cloud.BigQuery.V2;
using Google.Apis.Auth.OAuth2;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Linq;
using System.Data;

namespace Budget.TimerFunction.Gcp
{
    public class GcpCarbon
    {
        [FunctionName("TimerTrigger_GcpCarbon")]
        public void Run([TimerTrigger("%GCP_CarbonTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Carbon Foot Print function executed at: {DateTime.Now}");

                List<GcpBudgetModel.GcpBudget> objBudgetList = new List<GcpBudgetModel.GcpBudget>();


                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);
                DateTime datetime = DateTime.UtcNow;
                var date = new DateTime(datetime.Year, datetime.Month, 1);
                //var endDate = date.AddMonths(1).AddDays(-1);

                log.LogInformation($"GCP Carbon Foot Print Data Date {date.ToString("yyyy-MM-dd")}");

                DataTable dt= GetGcpCarbonFootPrintList(client, date.ToString("yyyy-MM-dd"), log);

                GcptoSql.SaveCarbonFootPrint(dt, date.ToString("yyyy-MM-dd"), log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex;
            }
        }
        public DataTable GetGcpCarbonFootPrintList(BigQueryClient client, string date, ILogger log)
        {
            var query = $"SELECT usage_month as UsageMonth," +
                $"project.number as ProjectNumber," +
                $"project.id as ProjectId," +
                $"service.id as ServiceId," +
                $"service.description as ServiceDesc," +
                $"location.location as Location," +
                $"location.region as Region," +
                $"carbon_footprint_kgCO2e.scope1 as Scope1," +
                $"carbon_footprint_kgCO2e.scope2.location_based as Scope2," +
                $"carbon_footprint_kgCO2e.scope3 as Scope3," +
                $"carbon_footprint_total_kgCO2e.location_based as TotalKgCO2eLocationBased," +
                $"(carbon_footprint_kgCO2e.scope1 + carbon_footprint_kgCO2e.scope2.location_based + carbon_footprint_kgCO2e.scope3 + carbon_footprint_total_kgCO2e.location_based) as TotalCarbonFootPrint " +                
                $"FROM `eygds-sandbox-cloud-359111.billing_info_1.carbon_footprint`";

            log.LogInformation($"GCP Budget query '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);
            log.LogInformation($"No of GCP Budget rows {results.TotalRows} returned");

            DataTable dt = new DataTable();
            dt.Columns.Add("Id");
            dt.Columns.Add("UsageMonth");
            dt.Columns.Add("ProjectNumber");
            dt.Columns.Add("ProjectId");
            dt.Columns.Add("ServiceId");
            dt.Columns.Add("ServiceDesc");
            dt.Columns.Add("Location");
            dt.Columns.Add("Region");
            dt.Columns.Add("Scope1");
            dt.Columns.Add("Scope2");
            dt.Columns.Add("Scope3");
            dt.Columns.Add("TotalKgCO2eLocationBased");
            dt.Columns.Add("TotalCarbonFootPrint");
            dt.Columns.Add("CreatedOn");          
           
            foreach (var data in results)
            {
                DataRow row = dt.NewRow();
                row["Id"] = null;
                row["UsageMonth"] = data["UsageMonth"];
                row["ProjectNumber"] = data["ProjectNumber"];
                row["ProjectId"] = data["ProjectId"];
                row["ServiceId"] = data["ServiceId"];
                row["ServiceDesc"] = data["ServiceDesc"];
                row["Location"] = data["Location"];
                row["Region"] = data["Region"];
                row["Scope1"] = Helper.ValidateDecimal(Convert.ToString(data["Scope1"]));
                row["Scope2"] = Helper.ValidateDecimal(Convert.ToString(data["Scope2"]));
                row["Scope3"] = Helper.ValidateDecimal(Convert.ToString(data["Scope3"]));
                row["TotalKgCO2eLocationBased"] = Helper.ValidateDecimal(Convert.ToString(data["TotalKgCO2eLocationBased"]));
                row["TotalCarbonFootPrint"] = Helper.ValidateDecimal(Convert.ToString(data["TotalCarbonFootPrint"]));
                row["CreatedOn"] = DateTime.UtcNow;
                dt.Rows.Add(row);
            }
            return dt;
        }
       
    }
}
