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
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Advisor Timer trigger function executed at: {DateTime.Now}");

                List<GCPAdvisorModel.GCPAdvisor> objAdvisorList = new List<GCPAdvisorModel.GCPAdvisor>();


                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);

                var objAdvisorRecommendation = GetGCPAdvisorRecommendationList(client, log);
                var objAdvisorInsight = GetGCPAdvisorInsightList(client, log);
                foreach (var objRecommendation in objAdvisorRecommendation)
                {
                    GCPAdvisorModel.GCPAdvisor objAdvisor=new GCPAdvisorModel.GCPAdvisor();
                    objAdvisor.ProjectNumber = objRecommendation.cloud_entity_id;
                    objAdvisor.Name = objRecommendation.name;
                    objAdvisor.Description = objRecommendation.description;
                    objAdvisor.LastRefreshDate = objRecommendation.last_refresh_time;
                    objAdvisor.Units = objRecommendation.primary_impact.cost_projection.cost.units;
                    objAdvisor.Nanos = objRecommendation.primary_impact.cost_projection.cost.nanos;
                    objAdvisor.CurrencyCode = objRecommendation.primary_impact.cost_projection.cost.currency_code;
                    objAdvisor.Type = objRecommendation.recommender;
                    objAdvisor.SubType = objRecommendation.recommender_subtype;
                    objAdvisor.Severity = Helper.GetSeverity(objRecommendation.priority);
                    objAdvisor.Category = objRecommendation.primary_impact.category;
                    objAdvisor.Location = objRecommendation.location;
                    objAdvisorList.Add(objAdvisor);
                }

                log.LogInformation($"GCP Advisor  total no of rows {objAdvisorList.Count} will be insert to sql table");
                GcptoSql.SaveGcpAdvisor(objAdvisorList, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw new Exception(ex.Message, ex);
            }
        }
        public List<GCPAdvisorModel.GCPAdvisorRecommendation> GetGCPAdvisorRecommendationList(BigQueryClient client, ILogger log)
        {
            List<GCPAdvisorModel.GCPAdvisorRecommendation> objAdvisor = new List<GCPAdvisorModel.GCPAdvisorRecommendation>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Advisor No of Recommendation rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPAdvisorModel.GCPAdvisorRecommendation>(gcpBillingJsonData);
                objAdvisor.Add(result);
            }
            return objAdvisor;
        }
        public List<GCPAdvisorModel.GCPAdvisorInsight> GetGCPAdvisorInsightList(BigQueryClient client, ILogger log)
        {
            List<GCPAdvisorModel.GCPAdvisorInsight> objAdvisor = new List<GCPAdvisorModel.GCPAdvisorInsight>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorInsightsTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Advisor no of Insight rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPAdvisorModel.GCPAdvisorInsight>(gcpBillingJsonData);
                objAdvisor.Add(result);
            }
            return objAdvisor;
        }
    }
}
