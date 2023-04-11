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
    public class TimerTrigger_GcpBillingCost
    {
        private readonly IGcpBillingCost _gcpBillingCost;
        private readonly IGcpCredentials _gcpCredentials;

        public TimerTrigger_GcpBillingCost(IGcpBillingCost gcpBillingCost,
            IGcpCredentials gcpCredentials)
        {
            _gcpBillingCost = gcpBillingCost;
            _gcpCredentials = gcpCredentials;
        }
        [FunctionName("TimerTrigger_GcpBillingCost")]
        public void Run([TimerTrigger("%GCP_DailyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {               
                log.LogInformation($"GCP Billing Cost Timer trigger function executed at: {DateTime.Now}");
                var client = _gcpCredentials.GcpAuthentication();
                _gcpBillingCost.PutGcpBillingCost(client);
                log.LogInformation($"GCP Billing Cost function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Billing Cost error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Billing Cost Error - {ex.Message}", ex);
            }
        }
    }
}
