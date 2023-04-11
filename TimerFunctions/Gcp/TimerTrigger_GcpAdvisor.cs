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
    public class TimerTrigger_GcpAdvisor
    {
        private readonly IGcpAdvisorRecommendation _gcpAdvisorRecommendation;
        private readonly IGcpCredentials _gcpCredentials;

        public TimerTrigger_GcpAdvisor(IGcpAdvisorRecommendation gcpAdvisorRecommendation,
            IGcpCredentials gcpCredentials)
        {
            _gcpAdvisorRecommendation = gcpAdvisorRecommendation;
            _gcpCredentials = gcpCredentials;
        }

        [FunctionName("TimerTrigger_GcpAdvisor")]
        public void Run([TimerTrigger("%GCP_WeeklyTimer%")] TimerInfo myTimer,ILogger log)
        {
            try
            {
                log.LogInformation($"GCP Advisor Timer trigger function executed at: {DateTime.Now}");            

               var client= _gcpCredentials.GcpAuthentication();

                _gcpAdvisorRecommendation.PutGcpAdvisorRecommendation(client);

                log.LogInformation($"GCP Advisor function Process Completed..");

            }
            catch (Exception ex)
            {
                log.LogError(ex, $"Gcp Advisor error occurred in the Timer Trigger Function - {ex.Message}");
                throw new Exception($"Gcp Advisor Error - {ex.Message}", ex);
            }
        }
    }
}
