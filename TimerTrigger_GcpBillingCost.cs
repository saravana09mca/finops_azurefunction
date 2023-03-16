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

namespace Budget.TimerFunction
{
    public class TimerTrigger_GcpBillingCost
    {
        [FunctionName("TimerTrigger_GcpBillingCost")]
        public void Run([TimerTrigger("%GCPBillingCostTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Timer trigger function executed at: {DateTime.Now}");

                log.LogInformation($"ConfigStore Values of projectId:{ConfigStore.GCP_ProjectId}, datasetId:{ConfigStore.GCP_DataSetId}, tableId:{ConfigStore.GCP_TableId}");

                List<GCPBillingCostModel.GCPBillingCost> objbilling = new List<GCPBillingCostModel.GCPBillingCost>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP_BlobContrainerName, ConfigStore.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP_ProjectId, credentials);

                if (!ConfigStore.GCP_IsManualDateRange)
                {
                    var date = DateTime.UtcNow.AddDays(ConfigStore.GCP_DataDaysDiff).ToString("yyyy-MM-dd");
                    ConfigStore.GCP_FromDate = ConfigStore.GCP_ToDate = date;
                }

                log.LogInformation($"GCP Billing Records from {ConfigStore.GCP_FromDate}");

                objbilling = GetGCPBillingList(client,log);

                GcptoSql.SaveBillingCost(objbilling, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GCPBillingCostModel.GCPBillingCost> GetGCPBillingList(BigQueryClient client, ILogger log)
        {
            List<GCPBillingCostModel.GCPBillingCost> objbilling = new List<GCPBillingCostModel.GCPBillingCost>();
            // Build the query
            var query = $"SELECT distinct " +
                $"billing_account_id as BillingAccountId," +
                $"service.id as ServiceId," +
                $"service.description as ServiceDesc," +
                $"sku.id as SkuId," +
                $"sku.description as SkuDesc," +
                $"project.id as ProjectId," +
                $"project.number as ProjectNumber," +
                $"project.name as ProjectName," +
                $"usage_start_time as UsageStartDate," +
                $"usage_end_time as Date," +
                $"export_time as ExportTime," +
                $"location.location as Location," +
                $"location.region as Region," +
                $"resource.name as ResourceName," +
                $"resource.global_name as ResourceId," +
                $"cost as Cost," +
                $"(cost/currency_conversion_rate) as CostUsd," +
                $"currency as Currency," +
                $"currency_conversion_rate as CurrencyConversionRate" +
                $" FROM {ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId} where Date(export_time)>='{ConfigStore.GCP_FromDate}'";      

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"No of GCP billing rows {results.TotalRows} returned");

            Dictionary<string, object> rowoDict;
            List<string> fields = new List<string>();
            foreach (var col in results.Schema.Fields)
            {
                fields.Add(col.Name);
            }
            foreach (var row in results)
            {
                rowoDict = new Dictionary<string, object>();
                foreach (var col in fields)
                {
                    rowoDict.Add(col, row[col]);
                }
                string gcpBillingJsonData = Newtonsoft.Json.JsonConvert.SerializeObject(rowoDict);
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPBillingCostModel.GCPBillingCost>(gcpBillingJsonData);
                objbilling.Add(result);
            }
            return objbilling;
        }
    }
}
