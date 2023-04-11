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
using AzureFunction.Services.Gcp;

namespace Budget.TimerFunction.Gcp
{
    public class TimerTrigger_GcpTags
    {
        private readonly IGcpTags _gcpTags;
        private readonly IGcpCredentials _gcpCredentials;
        public TimerTrigger_GcpTags(IGcpTags gcpTags,
            IGcpCredentials gcpCredentials)
        {
            _gcpTags = gcpTags;
            _gcpCredentials = gcpCredentials;
        }
        [FunctionName("TimerTrigger_GcpTags")]
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Tags trigger function executed at: {DateTime.Now}");
                
                var client = _gcpCredentials.GcpAuthentication();
                _gcpTags.PutGcpTags(client);

                log.LogInformation($"GCP Tags function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Tags error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Tags Error - {ex.Message}", ex);
            }
        }
       
    }
}
