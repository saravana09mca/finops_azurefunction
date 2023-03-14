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

                List<GCPBillingModel> objbilling = new List<GCPBillingModel>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP_ContrainerName,ConfigStore.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP_ProjectId, credentials);

                if (!ConfigStore.GCP_IsManualDateRange)
                {
                    var date = DateTime.UtcNow.AddDays(ConfigStore.GCP_DataDaysDiff).ToString("yyyy-MM-dd");
                    ConfigStore.GCP_FromDate = ConfigStore.GCP_ToDate = date;
                }
                if (GcptoSql.CheckBillingCostDateExists(ConfigStore.GCP_FromDate, ConfigStore.GCP_ToDate))
                {
                    log.LogError($"Data already exists from {ConfigStore.GCP_FromDate} to {ConfigStore.GCP_ToDate}");
                    return;
                }

                log.LogInformation($"GCP Billing Records Date Range from {ConfigStore.GCP_FromDate} to {ConfigStore.GCP_ToDate}");

                objbilling = GetGCPBillingList(client,log);

                GcptoSql.SaveBillingCost(objbilling, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GCPBillingModel> GetGCPBillingList(BigQueryClient client, ILogger log)
        {
            List<GCPBillingModel> objbilling = new List<GCPBillingModel>();
            // Build the query
            var query = $"SELECT (cost/currency_conversion_rate) as CostUsd,* FROM {ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId} where Date(export_time) between '{ConfigStore.GCP_FromDate}' and '{ConfigStore.GCP_ToDate}'";

            //var query = $"SELECT sum(cost),cast(usage_end_time as date)  FROM {ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId} where project.name='int-ops-cloud-0223' and project.id='int-ops-cloud-0223' and project.number='915729704939' and cast(usage_end_time as date) between '2023-03-1' and '2023-03-10' group by cast(usage_end_time as date)";

            //var query = $"SELECT service.description FROM {ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId} where project.name='int-ops-cloud-0223' and project.id='int-ops-cloud-0223' and project.number='915729704939' and cast(usage_end_time as date) between '2023-03-7' and '2023-03-7' and sku.description='Networking Traffic Egress GCP Replication within Asia'";

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPBillingModel>(gcpBillingJsonData);
                objbilling.Add(result);
            }
            return objbilling;
        }
    }
}
