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
    public class TimerTrigger_GcpBudget
    {
        private readonly IGcpBudget _gcpBudget;
        private readonly IGcpCredentials _gcpCredentials;
        public TimerTrigger_GcpBudget(IGcpBudget gcpBudget,
            IGcpCredentials gcpCredentials)
        {
            _gcpBudget = gcpBudget;
            _gcpCredentials = gcpCredentials;
        }

        [FunctionName("TimerTrigger_GcpBudget")]
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Budget function executed at: {DateTime.Now}");

                var client = _gcpCredentials.GcpAuthentication();

                _gcpBudget.PutGcpBudget(client);

                log.LogInformation($"GCP Budget function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Budget error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Budget Error - {ex.Message}", ex);
            }
        }
    }
}
