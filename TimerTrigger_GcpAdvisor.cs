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


namespace Budget.TimerFunction
{
    public class TimerTrigger_GcpAdvisor
    {
        [FunctionName("TimerTrigger_GcpAdvisor")]
        public void Run([TimerTrigger("%GCPAdvisorTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Advisor Timer trigger function executed at: {DateTime.Now}");

                List<GCPAdvisorModel> objAdvisor = new List<GCPAdvisorModel>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP_ContrainerName,ConfigStore.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP_ProjectId, credentials);

                log.LogInformation($"GCP Billing Records Date Range from {ConfigStore.GCP_FromDate} to {ConfigStore.GCP_ToDate}");

                objAdvisor = GetGCPAdvisorList(client,log);

                GcptoSql.SaveGcpAdvisor(objAdvisor, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GCPAdvisorModel> GetGCPAdvisorList(BigQueryClient client, ILogger log)
        {
            List<GCPAdvisorModel> objbilling = new List<GCPAdvisorModel>();
            // Build the query
            var query = "SELECT  * FROM eygds-sandbox-cloud-359111.billing_info_1.recommendations_export";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"No of GCP Advisor rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPAdvisorModel>(gcpBillingJsonData);
                objbilling.Add(result);
            }
            return objbilling;
        }
    }
}
