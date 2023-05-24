using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;

using Budget.TimerFunction.GCPAdvisorModel;
using Microsoft.Extensions.Logging;
using System.Data;
using Microsoft.OData.Edm;

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

            _logger.LogInformation("PutGcpOrphanedData start");
            try
            {

                DataTable dt = new DataTable();
                dt.Columns.Add("Id");
                dt.Columns.Add("ProjectId");
                dt.Columns.Add("ProjectNumber");
                dt.Columns.Add("ProjectName");
                dt.Columns.Add("Location");
                dt.Columns.Add("ResourceName");
                dt.Columns.Add("ResourceType");
                dt.Columns.Add("Description");
                dt.Columns.Add("Date");
                dt.Columns.Add("InsertDate");
                dt = GetOrphanedResources(client, dt);
                dt = GetOrphanedResourcesInsight(client, dt);

                _logger.LogInformation($"GCP Orphaned data rows {dt.Rows.Count} returned");
                _gcpSql.SaveGcpOrphaned(dt);
            }
            catch (Exception)
            {
                throw;
            }
        }        
        public DataTable GetOrphanedResources(BigQueryClient client, DataTable dt)
        {


            var query = $"SELECT * FROM `eygds-sandbox-cloud-359111.billing_info_2.orphaned_resource_3`";
            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);
            foreach (var data in results)
            {
                DataRow row = dt.NewRow();
                row["Id"] = null;
                row["ProjectId"] = data["project_id"];
                row["ProjectNumber"] = data["project_number"];
                row["ProjectName"] = data["project_name"];
                row["Location"] = data["location"];
                row["ResourceName"] = data["resource_name"];
                row["Date"] = data["creationtimestamp"];
                row["ResourceType"] = data["resource_type"];
                row["Description"] = data["description"];
                row["InsertDate"] = DateTime.UtcNow;
                dt.Rows.Add(row);
            }
            return dt;
        }
        public DataTable GetOrphanedResourcesInsight(BigQueryClient client, DataTable dt)
        {


            var query = $"SELECT * FROM `eygds-sandbox-cloud-359111.billing_info_2.orphaned_resource_4`";
            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);
            foreach (var data in results)
            {
                DataRow row = dt.NewRow();
                row["Id"] = null;
                row["ProjectId"] = data["id"];
                row["ProjectNumber"] = data["number"];
                row["ProjectName"] = data["name"];
                row["Location"] = data["location"];
                row["ResourceName"] = data["resource_name"];
                row["Date"] = data["time"];
                row["ResourceType"] = data["resource_type"];
                row["Description"] = data["description"];
                row["InsertDate"] = DateTime.UtcNow;
                dt.Rows.Add(row);
            }
            return dt;
        }
    }
}
