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
using AzureFunction.Services.Gcp;

namespace Budget.TimerFunction.Gcp
{
    public class TimerTrigger_GcpOrphaned
    {
        private readonly IGcpOrphaned _gcpOrphaned;
        private readonly IGcpCredentials _gcpCredentials;
        public TimerTrigger_GcpOrphaned(IGcpOrphaned gcpOrphaned,
            IGcpCredentials gcpCredentials)
        {
            _gcpOrphaned = gcpOrphaned;
            _gcpCredentials = gcpCredentials;
        }
        [FunctionName("TimerTrigger_GcpOrphaned")]
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Orphaned Timer trigger function executed at: {DateTime.Now}");

                var client = _gcpCredentials.GcpAuthentication();
                _gcpOrphaned.PutGcpOrphanedData(client);

                log.LogInformation($"GCP Orphaned function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Orphaned error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Orphaned Error - {ex.Message}", ex);
            }
        }

    }
}
