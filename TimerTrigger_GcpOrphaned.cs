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
    public class TimerTrigger_GcpOrphaned
    {
        [FunctionName("TimerTrigger_GcpOrphaned")]
        public void Run([TimerTrigger("%GCPOrphanedTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Orphaned Timer trigger function executed at: {DateTime.Now}");

                List<GCPAdvisorModel.GCPAdvisor> objOrphanedList = new List<GCPAdvisorModel.GCPAdvisor>();
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);

                var objOrphanedDataRecommendation = GetGCPOrphanedRecommendationList(client, log);
                var objOrphanedDataInsight = GetGCPOrphanedInsightList(client, log);
                foreach (var objRecommendation in objOrphanedDataRecommendation)
                {
                    GCPAdvisorModel.GCPAdvisor objOrphanedData=new GCPAdvisorModel.GCPAdvisor();
                    objOrphanedData.ProjectNumber = objRecommendation.cloud_entity_id;
                    objOrphanedData.Name = objRecommendation.name;
                    objOrphanedData.Description = objRecommendation.description;
                    objOrphanedData.LastRefreshDate = objRecommendation.last_refresh_time;          
                    objOrphanedData.Type = objRecommendation.recommender;                   
                    objOrphanedData.Category = objRecommendation.primary_impact.category;
                    objOrphanedData.Location = objRecommendation.location;
                    objOrphanedList.Add(objOrphanedData);
                }

                foreach (var objInsight in objOrphanedDataInsight)
                {
                    GCPAdvisorModel.GCPAdvisor objOrphanedData = new GCPAdvisorModel.GCPAdvisor();
                    objOrphanedData.ProjectNumber = objInsight.cloud_entity_id;
                    objOrphanedData.Name = objInsight.name;
                    objOrphanedData.Description = objInsight.description;
                    objOrphanedData.LastRefreshDate = objInsight.last_refresh_time;                   
                    objOrphanedData.Type = objInsight.insight_type;                    
                    objOrphanedData.Category = objInsight.category;
                    objOrphanedData.Location = objInsight.location;
                    objOrphanedList.Add(objOrphanedData);
                }

                log.LogInformation($"GCP Orphaned  total no of rows {objOrphanedList.Count} will be insert to sql table");
                GcptoSql.SaveGcpOrphaned(objOrphanedList, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw new Exception(ex.Message, ex);
            }
        }
        public List<GCPAdvisorModel.GCPAdvisorRecommendation> GetGCPOrphanedRecommendationList(BigQueryClient client, ILogger log)
        {
            List<GCPAdvisorModel.GCPAdvisorRecommendation> objOrphanedData = new List<GCPAdvisorModel.GCPAdvisorRecommendation>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorTableId}` where recommender in ('google.cloudsql.instance.IdleRecommender'," +
                $"'google.compute.image.IdleResourceRecommender'," +
                $"'google.compute.address.IdleResourceRecommender'," +
                $"'google.compute.disk.IdleResourceRecommender'," +
                $"'google.compute.instance.IdleResourceRecommender')";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Orphaned No of Recommendation rows {results.TotalRows} returned");

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
                objOrphanedData.Add(result);
            }
            return objOrphanedData;
        }
        public List<GCPAdvisorModel.GCPAdvisorInsight> GetGCPOrphanedInsightList(BigQueryClient client, ILogger log)
        {
            List<GCPAdvisorModel.GCPAdvisorInsight> objOrphanedData = new List<GCPAdvisorModel.GCPAdvisorInsight>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorInsightsTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Orphaned no of Insight rows {results.TotalRows} returned");

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
                objOrphanedData.Add(result);
            }
            return objOrphanedData;
        }
    }
}
