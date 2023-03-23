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
    public class TimerTrigger_GcpUtilization
    {
        [FunctionName("TimerTrigger_GcpUtilization")]
        public void Run([TimerTrigger("%GCPUtilizationTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Utilization function executed at: {DateTime.Now}");

                List<GCPUtilizationModel.GCPUtilization> objUtilization = new List<GCPUtilizationModel.GCPUtilization>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);
                DateTime Date = DateTime.UtcNow.Date.AddDays(ConfigStore.GCP.GCP_UtilizationDataDateDiff); //Get previous day start time

                log.LogInformation($"GCP Utilization Data Date {Date.ToString("yyyy-MM-dd")}");

                objUtilization = GetGCPUtilizationList(client, Date.ToString("yyyy-MM-dd"), log);

                GcptoSql.SaveGcpUtilization(objUtilization,Date.ToString("yyyy-MM-dd"), log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GCPUtilizationModel.GCPUtilization> GetGCPUtilizationList(BigQueryClient client,string date, ILogger log)
        {   
            List<GCPUtilizationModel.GCPUtilizationList> objUtilization = new List<GCPUtilizationModel.GCPUtilizationList>();
            // Build the query
            var query = $"SELECT * FROM `{ConfigStore.GCP.GCP_UtilizationProjectId}.{ConfigStore.GCP.GCP_UtilizationDatasetId}.{ConfigStore.GCP.GCP_UtilizationTableId}` where cast(pointData.timeInterval.start_time as date)='{date}'";

            log.LogInformation($"GCP Utilization query '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"No of GCP Utilization rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPUtilizationModel.GCPUtilizationList>(gcpBillingJsonData);
                objUtilization.Add(result);
            }
            List<GCPUtilizationModel.GCPUtilization> objUtilizationList=new List<GCPUtilizationModel.GCPUtilization>();
            if (objUtilization.Count > 0)
            {
                var listUtilization = objUtilization.GroupBy(x => new { metricName = x.metricName.Replace("compute.googleapis.com/",""), projectId = x.timeSeriesDescriptor.labels[0].value, instanceId = x.timeSeriesDescriptor.labels[1].value as string }).ToList();
                foreach (var item in listUtilization)
                {
                    GCPUtilizationModel.GCPUtilization objData=new GCPUtilizationModel.GCPUtilization();
                    objData.MetricName = item.Key.metricName;
                    objData.ProjectId = item.Key.projectId;
                    objData.InstanceId = item.Key.instanceId;
                    objData.Date = date;
                    objData.AvgUtilization = objUtilization.Where(x => x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Average(p => p.pointData.values.double_value);
                    objData.MaxUtilization = objUtilization.Where(x =>x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Max(p => p.pointData.values.double_value);
                    objData.MinUtilization = objUtilization.Where(x => x.timeSeriesDescriptor.labels[0].value == item.Key.projectId && x.timeSeriesDescriptor.labels[1].value == item.Key.instanceId).Min(p => p.pointData.values.double_value);
                    objUtilizationList.Add(objData);
                }
            }
            log.LogInformation($"GCP Utilization rows {objUtilizationList.Count} grouped");
            return objUtilizationList;
        }
    }
}
