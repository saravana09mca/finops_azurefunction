using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Budget.TimerFunction.GCPUtilizationModel;

namespace AzureFunction.Services.Gcp
{
    public class GcpUtilizationService : IGcpUtilization
    {

        private readonly ILogger<GcpTagsService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpUtilizationService(ILogger<GcpTagsService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpUtilization(BigQueryClient client)
        {
            List<GCPUtilization> objUtilization = new List<GCPUtilization>();
            _logger.LogInformation("PutGcpTags start");
            try
            {

                DateTime Date = DateTime.UtcNow.Date.AddDays(ConfigStore.GCP.GCP_UtilizationDataDateDiff); //Get previous day start time

                _logger.LogInformation($"GCP Utilization Data {Date.ToString("yyyy-MM-dd")}");

                objUtilization = GetGCPUtilizationList(client, Date.ToString("yyyy-MM-dd"));

                _gcpSql.SaveGcpUtilization(objUtilization, Date.ToString("yyyy-MM-dd"));
            }
            catch(Exception)
            {
                throw;
            }
        }
        public List<GCPUtilization> GetGCPUtilizationList(BigQueryClient client, string date)
        {
            List<GCPUtilizationList> objUtilization = new List<GCPUtilizationList>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_UtilizationProjectId}.{ConfigStore.GCP.GCP_UtilizationDatasetId}.{ConfigStore.GCP.GCP_UtilizationTableId}` where cast(pointData.timeInterval.start_time as date)='{date}'";

            _logger.LogInformation($"GCP Utilization query : '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Utilization rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPUtilizationList>(gcpBillingJsonData);
                objUtilization.Add(result);
            }
            List<GCPUtilization> objUtilizationList = new List<GCPUtilization>();
            if (objUtilization.Count > 0)
            {
                var listUtilization = objUtilization.GroupBy(x => new { metricName = x.metricName.Replace("compute.googleapis.com/", ""), projectId = x.timeSeriesDescriptor.labels[0].value, instanceId = x.timeSeriesDescriptor.labels[1].value as string }).ToList();
                foreach (var item in listUtilization)
                {
                    GCPUtilization objData = new GCPUtilization();
                    objData.MetricName = item.Key.metricName;
                    objData.ProjectId = item.Key.projectId;
                    objData.InstanceId = item.Key.instanceId;
                    objData.Date = date;
                    objData.AvgUtilization = objUtilization.Where(x => x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Average(p => p.pointData.values.double_value);
                    objData.MaxUtilization = objUtilization.Where(x => x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Max(p => p.pointData.values.double_value);
                    objData.MinUtilization = objUtilization.Where(x => x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Min(p => p.pointData.values.double_value);
                    objUtilizationList.Add(objData);
                }
            }
            _logger.LogInformation($"GCP Utilization rows {objUtilizationList.Count} grouped");
            return objUtilizationList;
        }
    }
}
