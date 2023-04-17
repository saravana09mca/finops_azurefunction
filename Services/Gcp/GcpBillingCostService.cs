using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Microsoft.AspNetCore.Http;
using Budget.TimerFunction.GCPBillingCostModel;

namespace AzureFunction.Services.Gcp
{
    public class GcpBillingCostService : IGcpBillingCost
    {

        private readonly ILogger<GcpBillingCostService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpBillingCostService(ILogger<GcpBillingCostService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpBillingCost(BigQueryClient client)
        {

            _logger.LogInformation("PutGcpBillingCost start");
          
            try
            {
                List<GCPBillingCost> objbilling = new List<GCPBillingCost>();
                var date = DateTime.UtcNow.AddDays(ConfigStore.GCP.GCP_DataDaysDiff).ToString("yyyy-MM-dd");
                ConfigStore.GCP.GCP_FromDate = ConfigStore.GCP.GCP_ToDate = date;


                _logger.LogInformation($"GCP Billing Records from {ConfigStore.GCP.GCP_FromDate}");

                objbilling = GetGCPBillingList(client);

                _gcpSql.SaveBillingCost(objbilling);
            }
            catch(Exception)
            {
                throw;
            }
           
        }

        public List<GCPBillingCost> GetGCPBillingList(BigQueryClient client)
        {
            List<GCPBillingCost> objbilling = new List<GCPBillingCost>();
            // Build the query
            var query = $"SELECT distinct " +
                $"billing_account_id as BillingAccountId," +
                $"service.id as ServiceId," +
                $"service.description as ServiceDesc," +
                $"sku.id as SkuId," +
                $"sku.description as SkuDesc," +
                $"project.id as ProjectId," +
                $"project.number as ProjectNumber," +
                $"project.name as ProjectName," +
                $"usage_start_time as UsageStartDate," +
                $"usage_end_time as Date," +
                $"export_time as ExportTime," +
                $"location.location as Location," +
                $"location.region as Region," +
                $"resource.name as ResourceName," +
                $"resource.global_name as ResourceId," +
                $"cost as Cost," +
                $"(cost/currency_conversion_rate) as CostUsd," +
                $"currency as Currency," +
                $"currency_conversion_rate as CurrencyConversionRate" +
                $" FROM {ConfigStore.GCP.GCP_ProjectId}.{ConfigStore.GCP.GCP_DataSetId}.{ConfigStore.GCP.GCP_TableId} where Date(export_time)>='{ConfigStore.GCP.GCP_FromDate}'";

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Billing rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GCPBillingCost>(gcpBillingJsonData);
                objbilling.Add(result);
            }
            return objbilling;
        }
    }
}
