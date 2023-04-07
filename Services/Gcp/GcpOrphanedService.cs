using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;

using Budget.TimerFunction.GCPAdvisorModel;
using Microsoft.Extensions.Logging;

namespace AzureFunction.Services.Gcp
{
    public class GcpOrphanedService : IGcpOrphaned
    {

        private readonly ILogger<GcpOrphanedService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpOrphanedService(ILogger<GcpOrphanedService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpOrphanedData(BigQueryClient client)
        {

            List<GCPAdvisor> objOrphanedList = new List<GCPAdvisor>();
            _logger.LogInformation("PutGcpOrphanedData start");
            try
            {

                var objOrphanedDataRecommendation = GetGCPOrphanedRecommendationList(client);
                var objOrphanedDataInsight = GetGCPOrphanedInsightList(client);
                foreach (var objRecommendation in objOrphanedDataRecommendation)
                {
                    GCPAdvisor objOrphanedData = new GCPAdvisor();
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
                    GCPAdvisor objOrphanedData = new GCPAdvisor();
                    objOrphanedData.ProjectNumber = objInsight.cloud_entity_id;
                    objOrphanedData.Name = objInsight.name;
                    objOrphanedData.Description = objInsight.description;
                    objOrphanedData.LastRefreshDate = objInsight.last_refresh_time;
                    objOrphanedData.Type = objInsight.insight_type;
                    objOrphanedData.Category = objInsight.category;
                    objOrphanedData.Location = objInsight.location;
                    objOrphanedList.Add(objOrphanedData);
                }

                _logger.LogInformation($"GCP Orphaned data rows {objOrphanedList.Count} returned");
                _gcpSql.SaveGcpOrphaned(objOrphanedList);
            }
            catch(Exception)
            {
                throw;
            }
        }
        public List<GCPAdvisorRecommendation> GetGCPOrphanedRecommendationList(BigQueryClient client)
        {
            List<GCPAdvisorRecommendation> objOrphanedData = new List<GCPAdvisorRecommendation>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorTableId}` where recommender in ('google.cloudsql.instance.IdleRecommender'," +
                $"'google.compute.image.IdleResourceRecommender'," +
                $"'google.compute.address.IdleResourceRecommender'," +
                $"'google.compute.disk.IdleResourceRecommender'," +
                $"'google.compute.instance.IdleResourceRecommender')";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Orphaned No of Recommendation rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPAdvisorRecommendation>(gcpBillingJsonData);
                objOrphanedData.Add(result);
            }
            return objOrphanedData;
        }
        public List<GCPAdvisorInsight> GetGCPOrphanedInsightList(BigQueryClient client)
        {
            List<GCPAdvisorInsight> objOrphanedData = new List<GCPAdvisorInsight>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorInsightsTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Orphaned Insight rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPAdvisorInsight>(gcpBillingJsonData);
                objOrphanedData.Add(result);
            }
            return objOrphanedData;
        }
    }
}
