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
using Budget.TimerFunction.GCPUtilizationModel;

namespace Budget.TimerFunction.Gcp
{
    public class TimerTrigger_GcpUtilization
    {
        private readonly IGcpUtilization _gcpUtilization;

        private readonly IGcpCredentials _gcpCredentials;
        public TimerTrigger_GcpUtilization(IGcpUtilization gcpUtilization,
            IGcpCredentials gcpCredentials)
        {
            _gcpUtilization = gcpUtilization;
            _gcpCredentials = gcpCredentials;
        }
        [FunctionName("TimerTrigger_GcpUtilization")]
        public void Run([TimerTrigger("%GCP_DailyTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Utilization function executed at: {DateTime.Now}");

                var client = _gcpCredentials.GcpAuthentication();
                _gcpUtilization.PutGcpUtilization(client);

                log.LogInformation($"GCP Utilization function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Utilization error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Utilization Error - {ex.Message}", ex);
            }
        }
        
    }
}
