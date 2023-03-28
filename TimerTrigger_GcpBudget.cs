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
    public class GcpBudget
    {
        [FunctionName("TimerTrigger_GcpBudget")]
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Budget function executed at: {DateTime.Now}");

                List<GcpBudgetModel.GcpBudget> objBudgetList = new List<GcpBudgetModel.GcpBudget>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);
                DateTime datetime = DateTime.UtcNow;
                var date = new DateTime(datetime.Year, datetime.Month, 1);
                //var endDate = date.AddMonths(1).AddDays(-1);

                log.LogInformation($"GCP Utilization Data Date {date.ToString("yyyy-MM-dd")}");

                objBudgetList = GetGCPBudgetList(client, date.ToString("yyyy-MM-dd"), log);

                GcptoSql.SaveGcpBudget(objBudgetList, date.ToString("yyyy-MM-dd"), log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GcpBudgetModel.GcpBudget> GetGCPBudgetList(BigQueryClient client,string date, ILogger log)
        {   
            List<GcpBudgetModel.GcpBudget> objBudgetList = new List<GcpBudgetModel.GcpBudget>();
            // Build the query
            var query = $"SELECT distinct data FROM `{ConfigStore.GCP.GCP_BudgetProjectId}.{ConfigStore.GCP.GCP_BudgetDatasetId}.{ConfigStore.GCP.GCP_BudgetTableId}` where DATE(REGEXP_REPLACE(JSON_EXTRACT(data, '$.costIntervalStart'),'\"', ''))>='{date}'";

            log.LogInformation($"GCP Budget query '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"No of GCP Budget rows {results.TotalRows} returned");

           
            foreach (var row in results)
            {
                if (!string.IsNullOrEmpty(row["data"].ToString()))
                {
                    var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpBudgetModel.GcpBudget>(row["data"].ToString());
                    objBudgetList.Add(result);
                }
            }
           return objBudgetList;
        }
    }
}
