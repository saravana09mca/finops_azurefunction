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
using Budget.TimerFunction.Gcp;
using Budget.TimerFunction.GcpBudgetModel;
using System.Data;

namespace AzureFunction.Services.Gcp
{
    public class GcpCarbonService : IGcpCarbon
    {

        private readonly ILogger<GcpCarbonService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpCarbonService(ILogger<GcpCarbonService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpCarbon(BigQueryClient client)
        {

            _logger.LogInformation("PutGcpCarbon start");
            try
            {
                DateTime datetime = DateTime.UtcNow;
                var date = new DateTime(datetime.Year, datetime.Month, 1);
                //var endDate = date.AddMonths(1).AddDays(-1);

                _logger.LogInformation($"GCP Carbon FootPrint Data {date.ToString("yyyy-MM-dd")}");

                DataTable dt = GetGcpCarbonFootPrintList(client, date.ToString("yyyy-MM-dd"));

                _gcpSql.SaveCarbonFootPrint(dt, date.ToString("yyyy-MM-dd"));
            }
            catch(Exception)
            {
                throw;
            }
        }

        public DataTable GetGcpCarbonFootPrintList(BigQueryClient client, string date)
        {
            var query = $"SELECT usage_month as UsageMonth," +
                $"project.number as ProjectNumber," +
                $"project.id as ProjectId," +
                $"service.id as ServiceId," +
                $"service.description as ServiceDesc," +
                $"location.location as Location," +
                $"location.region as Region," +
                $"carbon_footprint_kgCO2e.scope1 as Scope1," +
                $"carbon_footprint_kgCO2e.scope2.location_based as Scope2," +
                $"carbon_footprint_kgCO2e.scope3 as Scope3," +
                $"carbon_footprint_total_kgCO2e.location_based as TotalKgCO2eLocationBased " +                
                $"FROM `eygds-sandbox-cloud-359111.billing_info_1.carbon_footprint` " +
                $"where Date(usage_month)>='{date}'";

            _logger.LogInformation($"GCP Budget query '{query}'");

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);
            _logger.LogInformation($"GCP Budget rows {results.TotalRows} returned");

            DataTable dt = new DataTable();
            dt.Columns.Add("Id");
            dt.Columns.Add("UsageMonth");
            dt.Columns.Add("ProjectNumber");
            dt.Columns.Add("ProjectId");
            dt.Columns.Add("ServiceId");
            dt.Columns.Add("ServiceDesc");
            dt.Columns.Add("Location");
            dt.Columns.Add("Region");
            dt.Columns.Add("Scope1");
            dt.Columns.Add("Scope2");
            dt.Columns.Add("Scope3");
            dt.Columns.Add("TotalKgCO2eLocationBased");
            dt.Columns.Add("CreatedOn");

            foreach (var data in results)
            {
                DataRow row = dt.NewRow();
                row["Id"] = null;
                row["UsageMonth"] = data["UsageMonth"];
                row["ProjectNumber"] = data["ProjectNumber"];
                row["ProjectId"] = data["ProjectId"];
                row["ServiceId"] = data["ServiceId"];
                row["ServiceDesc"] = data["ServiceDesc"];
                row["Location"] = data["Location"];
                row["Region"] = data["Region"];
                row["Scope1"] = Helper.ValidateDecimal(Convert.ToString(data["Scope1"]));
                row["Scope2"] = Helper.ValidateDecimal(Convert.ToString(data["Scope2"]));
                row["Scope3"] = Helper.ValidateDecimal(Convert.ToString(data["Scope3"]));
                row["TotalKgCO2eLocationBased"] = Helper.ValidateDecimal(Convert.ToString(data["TotalKgCO2eLocationBased"]));
                row["CreatedOn"] = DateTime.UtcNow;
                dt.Rows.Add(row);
            }
            return dt;
        }
    }
}
