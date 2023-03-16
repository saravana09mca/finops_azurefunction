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
    public class TimerTrigger_GcpUtilizatipn
    {
        [FunctionName("TimerTrigger_GcpUtilizatipn")]
        public void Run([TimerTrigger("%GCPUtilizationTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Utilization function executed at: {DateTime.Now}");

                List<GCPUtilizationModel.GCPUtilizationModel> objUtilization = new List<GCPUtilizationModel.GCPUtilizationModel>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP_BlobContrainerName, ConfigStore.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP_ProjectId, credentials);

                objUtilization = GetGCPUtilizationList(client,log);

                //GcptoSql.SaveGcpAdvisor(objUtilization, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GCPUtilizationModel.GCPUtilizationModel> GetGCPUtilizationList(BigQueryClient client, ILogger log)
        {
            List<GCPUtilizationModel.GCPUtilizationModel> objUtilization = new List<GCPUtilizationModel.GCPUtilizationModel>();
            // Build the query
            var query = "SELECT * FROM `eygds-sandbox-cloud-359111.metric_export1.mql_metrics1`";


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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPUtilizationModel.GCPUtilizationModel>(gcpBillingJsonData);
                objUtilization.Add(result);
            }
            var date = DateTime.UtcNow.AddDays(-2).ToString("yyyy-MM-dd");
            var listutil = objUtilization.GroupBy(pd => pd.pointData.timeInterval.start_time.ToString("yyyy-MM-dd") == date).ToList();

            //var maxdate = objUtilization.Max(x => x.pointData.timeInterval.start_time.ToString("yyyy-MM-dd"));
            //var mindate = objUtilization.Min(x => x.pointData.timeInterval.start_time.ToString("yyyy-MM-dd"));
            var avgValue = objUtilization.GroupBy(pd =>pd.pointData.timeInterval.start_time.ToString("yyyy-MM-dd")== date).Average(x=>x.Average(p=>p.pointData.values.double_value))*100;
            var maxValue = objUtilization.GroupBy(pd => pd.pointData.timeInterval.start_time.ToString("yyyy-MM-dd") == date).Max(x => x.Max(p => p.pointData.values.double_value)) * 100;
            var minValue = objUtilization.GroupBy(pd => pd.pointData.timeInterval.start_time.ToString("yyyy-MM-dd") == date).Max(x => x.Min(p => p.pointData.values.double_value)) * 100;
            return objUtilization;
        }
    }
}
