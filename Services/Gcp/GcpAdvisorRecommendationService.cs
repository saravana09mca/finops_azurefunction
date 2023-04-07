using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Budget.TimerFunction.GCPAdvisorModel;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;

namespace AzureFunction.Services.Gcp
{
    public class GcpAdvisorRecommendationService : IGcpAdvisorRecommendation
    {

        private readonly ILogger<GcpAdvisorRecommendationService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpAdvisorRecommendationService(ILogger<GcpAdvisorRecommendationService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpAdvisorRecommendation(BigQueryClient client)
        {
            _logger.LogInformation("GcpAdvisor","PutGcpAdvisorRecommendation start");
            try
            {
                List<GCPAdvisor> objAdvisorList = new List<GCPAdvisor>();
                var objAdvisorRecommendation = GetGCPAdvisorRecommendationList(client);
                var objAdvisorInsight = GetGCPAdvisorInsightList(client);
                foreach (var objRecommendation in objAdvisorRecommendation)
                {
                    GCPAdvisor objAdvisor = new GCPAdvisor();
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
                _logger.LogInformation($"GCP Advisor data count {objAdvisorList.Count}");
                _gcpSql.SaveGcpAdvisor(objAdvisorList);
            }
            catch(Exception)
            {
                throw;
            }
        }
        public List<GCPAdvisorRecommendation> GetGCPAdvisorRecommendationList(BigQueryClient client)
        {
            List<GCPAdvisorRecommendation> objAdvisor = new List<GCPAdvisorRecommendation>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Advisor Recommendation rows {results.TotalRows} returned");

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
                objAdvisor.Add(result);
            }
            return objAdvisor;
        }
        public List<GCPAdvisorInsight> GetGCPAdvisorInsightList(BigQueryClient client)
        {
            List<GCPAdvisorInsight> objAdvisor = new List<GCPAdvisorInsight>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_AdvisorProjectId}.{ConfigStore.GCP.GCP_AdvisorDatasetId}.{ConfigStore.GCP.GCP_AdvisorInsightsTableId}`";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Advisor Insight rows {results.TotalRows} returned");

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
                objAdvisor.Add(result);
            }
            return objAdvisor;
        }
    }
}
