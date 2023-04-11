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
using System.Data;
using AzureFunction.Services.Gcp;

namespace Budget.TimerFunction.Gcp
{
    public class TimerTrigger_GcpCarbon
    {
        private readonly IGcpCarbon _gcpCarbon;
        private readonly IGcpCredentials _gcpCredentials;
        public TimerTrigger_GcpCarbon(IGcpCarbon gcpCarbon,
            IGcpCredentials gcpCredentials)
        {
            _gcpCarbon = gcpCarbon;
            _gcpCredentials = gcpCredentials;   
        }
        [FunctionName("TimerTrigger_GcpCarbon")]
        public void Run([TimerTrigger("%GCP_CarbonTimer%")] TimerInfo myTimer, ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Carbon Foot Print function executed at: {DateTime.Now}");

                var client = _gcpCredentials.GcpAuthentication();

                _gcpCarbon.PutGcpCarbon(client);

                log.LogInformation($"GCP Carbon function Process Completed..");
            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Carbon error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Carbon Error - {ex.Message}", ex);
            }
        }
       
    }
}
