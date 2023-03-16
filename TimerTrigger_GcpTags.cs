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
    public class TimerTrigger_GcpTags
    {
        [FunctionName("TimerTrigger_GcpTags")]
        public void Run([TimerTrigger("%GCPTagsCostTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Tags trigger function executed at: {DateTime.Now}");

                log.LogInformation($"ConfigStore Values of projectId:{ConfigStore.GCP_ProjectId}, datasetId:{ConfigStore.GCP_DataSetId}, tableId:{ConfigStore.GCP_TableId}");

                List<GcpTagsModel.GcpTags> objTags = new List<GcpTagsModel.GcpTags>();
                
                
                GoogleCredential credentials = null;

                using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP_BlobContrainerName, ConfigStore.GCP_BlobFileName))
                {
                    credentials = GoogleCredential.FromStream(stream);
                }

                var client = BigQueryClient.Create(ConfigStore.GCP_ProjectId, credentials);

                log.LogInformation($"GCP Billing Records Date Range from {ConfigStore.GCP_FromDate} to {ConfigStore.GCP_ToDate}");

                List<GcpTagsModel.GcpTags> objTagsData = GetGCPTags(client,log);
                List<GcpTagsModel.GcpTags> objNoTagsData = GetGCPNoTags(client, log);
                foreach (var objtag in objNoTagsData) {
                    var checkTagdata = objTagsData.FirstOrDefault(x => x.ResourceId == objtag.ResourceId && x.ProjectId == objtag.ProjectId && x.ServiceId == objtag.ServiceId && x.ServiceDesc == objtag.ServiceDesc);
                    if (checkTagdata == null)
                    {
                        GcpTagsModel.GcpTags objTag = new GcpTagsModel.GcpTags();
                        objTag.ServiceId = objtag.ServiceId;
                        objTag.ServiceDesc = objtag.ServiceDesc;
                        objTag.ResourceId = objtag.ResourceId;
                        objTag.TagKey = objtag.TagKey;
                        objTag.TagValue = objtag.TagValue;
                        objTag.ProjectId = objtag.ProjectId;
                        objTagsData.Add(objTag);
                    }
                }
                log.LogInformation($"GCP Tags combined rows {objTagsData.Count} returned");
                GcptoSql.SaveGcpTags(objTagsData, log);
            }
            catch (Exception ex)
            {
                log.LogError(ex, ex.Message);
                throw ex; 
            }
        }
        public List<GcpTagsModel.GcpTags> GetGCPTags(BigQueryClient client, ILogger log)
        {
            List<GcpTagsModel.GcpTags> objTags = new List<GcpTagsModel.GcpTags>();
            //Build the query
            var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,h.key as TagKey,h.value as TagValue FROM `{ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId}`,UNNEST(tags) as h";


            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Tags rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpTagsModel.GcpTags>(gcpBillingJsonData);
                objTags.Add(result);
            }
            return objTags;
        }
        public List<GcpTagsModel.GcpTags> GetGCPNoTags(BigQueryClient client, ILogger log)
        {
            List<GcpTagsModel.GcpTags> objTags = new List<GcpTagsModel.GcpTags>();
            //Build the query
            //var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,h.key as TagKey,h.value as TagValue FROM `{ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId}`,UNNEST(tags) as h";
            var query = $"SELECT distinct project.id as ProjectId,service.id as ServiceId,service.description as ServiceDesc,resource.global_name as ResourceId,'' as TagKey,'' as TagValue FROM `{ConfigStore.GCP_ProjectId}.{ConfigStore.GCP_DataSetId}.{ConfigStore.GCP_TableId}`";

            // Run the query and get the results
            var results = client.ExecuteQuery(query, parameters: null);

            log.LogInformation($"GCP Empty Tags rows {results.TotalRows} returned");

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
                var result = Newtonsoft.Json.JsonConvert.DeserializeObject<GcpTagsModel.GcpTags>(gcpBillingJsonData);
                objTags.Add(result);
            }
            return objTags;
        }
    }
}
