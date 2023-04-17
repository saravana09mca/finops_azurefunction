using Budget.TimerFunction;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Logging;
using Budget.TimerFunction.GcpTagsModel;

namespace AzureFunction.Services.Gcp
{
    public class GcpTagsService : IGcpTags
    {

        private readonly ILogger<GcpTagsService> _logger;
        private readonly IGcpSql _gcpSql;


        public GcpTagsService(ILogger<GcpTagsService> logger, IGcpSql gcpSql)
        {
            _logger = logger;
            _gcpSql = gcpSql;
        }
        public void PutGcpTags(BigQueryClient client)
        {
            _logger.LogInformation("PutGcpTags start");
            try
            {

                List<GcpTags> objTagsData = GetGCPTags(client);
                List<GcpTags> objNoTagsData = GetGCPNoTags(client);
                foreach (var objtag in objNoTagsData)
                {
                    var checkTagdata = objTagsData.FirstOrDefault(x => x.ResourceId == objtag.ResourceId && x.ProjectId == objtag.ProjectId && x.ServiceId == objtag.ServiceId && x.ServiceDesc == objtag.ServiceDesc);
                    if (checkTagdata == null)
                    {
                        GcpTags objTag = new GcpTags();
                        objTag.ServiceId = objtag.ServiceId;
                        objTag.ServiceDesc = objtag.ServiceDesc;
                        objTag.ResourceId = objtag.ResourceId;
                        objTag.TagKey = objtag.TagKey;
                        objTag.TagValue = objtag.TagValue;
                        objTag.ProjectId = objtag.ProjectId;
                        objTagsData.Add(objTag);
                    }
                }
                _logger.LogInformation($"GCP Tags combined rows {objTagsData.Count} returned");
                _gcpSql.SaveGcpTags(objTagsData);
            }
            catch(Exception)
            {
                throw;
            }
        }
        public List<GcpTags> GetGCPTags(BigQueryClient client)
        {
            List<GcpTags> objTags = new List<GcpTags>();
            //Build the query
            var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,h.key as TagKey,h.value as TagValue FROM `{ConfigStore.GCP.GCP_ProjectId}.{ConfigStore.GCP.GCP_DataSetId}.{ConfigStore.GCP.GCP_TableId}`,UNNEST(tags) as h";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Tags rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpTags>(gcpBillingJsonData);
                objTags.Add(result);
            }
            return objTags;
        }
        public List<GcpTags> GetGCPNoTags(BigQueryClient client)
        {
            List<GcpTags> objTags = new List<GcpTags>();
            //Build the query
            //var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,h.key as TagKey,h.value as TagValue FROM `{ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId}`,UNNEST(tags) as h";
            var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,'' as TagKey,'' as TagValue FROM `{ConfigStore.GCP.GCP_ProjectId}.{ConfigStore.GCP.GCP_DataSetId}.{ConfigStore.GCP.GCP_TableId}`";

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            _logger.LogInformation($"GCP Empty Tags rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpTags>(gcpBillingJsonData);
                objTags.Add(result);
            }
            return objTags;
        }
    }
}
