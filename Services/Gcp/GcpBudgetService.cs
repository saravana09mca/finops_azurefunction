using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Budget.TimerFunction.GcpBudgetModel;

namespace AzureFunction.Services.Gcp
{
    public class GcpBudgetService:  IGcpBudget
    {

        private readonly ILogger<GcpBudgetService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpBudgetService(ILogger<GcpBudgetService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpBudget(BigQueryClient client)
        {

            _logger.LogInformation("PutGcpBudget start");
            try
            {

                List<GcpBudget> objBudgetList = new List<GcpBudget>();
                DateTime datetime = DateTime.UtcNow;
                var date = new DateTime(datetime.Year, datetime.Month, 1);
                //var endDate = date.AddMonths(1).AddDays(-1);

                _logger.LogInformation($"GCP Budget Date {date.ToString("yyyy-MM-dd")}");

                objBudgetList = GetGCPBudgetList(client, date.ToString("yyyy-MM-dd"));

                _gcpSql.SaveGcpBudget(objBudgetList, date.ToString("yyyy-MM-dd"));
            }
            catch(Exception)
            {
                throw;
            }
        }

        public List<GcpBudget> GetGCPBudgetList(BigQueryClient client, string date)
        {
            List<GcpBudget> objBudgetList = new List<GcpBudget>();
            // Build the query
            var query = $"SELECT distinct data FROM `{ConfigStore.GCP.GCP_BudgetProjectId}.{ConfigStore.GCP.GCP_BudgetDatasetId}.{ConfigStore.GCP.GCP_BudgetTableId}` where DATE(REGEXP_REPLACE(JSON_EXTRACT(data, '$.costIntervalStart'),'\"', ''))>='{date}'";

            _logger.LogInformation($"GCP Budget query: '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Budget rows {results.TotalRows} returned");


            foreach (var row in results)
            {
                if (!string.IsNullOrEmpty(row["data"].ToString()))
                {
                    var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpBudget>(row["data"].ToString());
                    objBudgetList.Add(result);
                }
            }
            return objBudgetList;
        }
    }
}
