using System;
using System.Data;
using System.Data.SqlClient;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using Microsoft.Azure.Management.Subscription;
using Microsoft.Azure.Management.Subscription.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.Rest;
using Newtonsoft.Json;

namespace Budget.TimerFunction.Azure
{
    public class CostInsightsData
    {
        [FunctionName("CostInsightsData")]
        public static async Task Run([TimerTrigger("%WeeklyTrigger%")]TimerInfo myTimer, ILogger log)
        {
            if (myTimer.IsPastDue)
            {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"C# Timer trigger function triggered at: {DateTime.Now}");
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            string[] respurceUrl = new string[] {"https://management.azure.com/.default"};

            //MSAL Authentication
            var clientApp = ConfidentialClientApplicationBuilder
            .Create(clientId: ConfigStore.AADClientId)
            .WithClientSecret(clientSecret: ConfigStore.AADClientSecret)
            .WithAuthority(new Uri (ConfigStore.AADAuthority))
            .Build();

            try
            {
                AuthenticationResult authResult = await clientApp.AcquireTokenForClient(respurceUrl).ExecuteAsync();

                //Web Api
                var httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);

                var credentials = new TokenCredentials(authResult.AccessToken);
                DataTable sourceData = new DataTable();
                DataRow row = null;

                sourceData.Columns.Add("Id").DefaultValue = null;
                sourceData.Columns.Add("SubscriptionID");
                sourceData.Columns.Add("SubscriptionName");
                sourceData.Columns.Add("InsightsId");
                sourceData.Columns.Add("InsightsName");
                sourceData.Columns.Add("InsightsType");
                sourceData.Columns.Add("Kind");
                sourceData.Columns.Add("EventDate");
                sourceData.Columns.Add("Severity");
                sourceData.Columns.Add("RGCount");
                sourceData.Columns.Add("IsAnomaly");
                sourceData.Columns.Add("Currency");
                sourceData.Columns.Add("ActualCost");
                sourceData.Columns.Add("Average");
                sourceData.Columns.Add("Minimum");
                sourceData.Columns.Add("Maximum");
                sourceData.Columns.Add("ExpectedValue");
                sourceData.Columns.Add("ExpectedMin");
                sourceData.Columns.Add("ExpectedMax");
                sourceData.Columns.Add("DeltaPercent");

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    Console.WriteLine("{0} \n", subscriptionIds);
                    if(subscription.State == SubscriptionState.Enabled)
                    {
                        DateTime baseDate = DateTime.Today;
                        var thisWeekStart = baseDate.AddDays(-(int)baseDate.DayOfWeek);
                        var lastWeekStart = thisWeekStart.AddDays(-7).ToString();
                        string weekStart = lastWeekStart.Split(' ')[0];
                        string startDate = Convert.ToDateTime(weekStart).ToString("yyyy-MM-dd");

                        var lastWeekEnd = thisWeekStart.AddSeconds(-1).ToString();
                        string weekEnd = lastWeekEnd.Split(' ')[0];
                        string endDate = Convert.ToDateTime(weekEnd).ToString("yyyy-MM-dd"); 

                        //call api to get the CostInsights
                        var insightsApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.CostManagement/insights?$filter=Properties/EventDate ge {startDate} and Properties/EventDate le {endDate}&$top=1000&api-version=2020-08-01-preview";
                        var insightJson = httpClient.GetAsync(insightsApiUrl).Result;
                        if (insightJson != null && insightJson.IsSuccessStatusCode == true)
                        {
                            var insightResult = insightJson.Content.ReadAsStringAsync().Result;
                            dynamic insightResponse = JsonConvert.DeserializeObject(insightResult);

                            foreach (var insight in insightResponse.values)
                            {
                                if(insight.kind == "NormalizedUsageAnomaly")
                                {
                                    row = sourceData.NewRow(); 

                                    row["SubscriptionID"] = subscriptionIds;
                                    row["SubscriptionName"] = subscription.DisplayName;
                                    row["InsightsId"] = insight.id;
                                    row["InsightsName"] = insight.name;
                                    row["InsightsType"] = insight.type;
                                    row["Kind"] = insight.kind;
                                    row["EventDate"] = insight.properties.eventDate;
                                    row["Severity"] = insight.properties.severity;
                                    if(insight.properties.details == null)
                                    {
                                        row["RGCount"] = 0;
                                    }
                                    else
                                    {
                                        row["RGCount"] = insight.properties.details.resourceGroups.count.total;
                                    }
                                    row["IsAnomaly"] = insight.properties.justification.isAnomaly;
                                    row["Currency"] = insight.properties.justification.currency;
                                    row["ActualCost"] = insight.properties.justification.actual;
                                    row["Average"] = insight.properties.justification.average;
                                    row["Minimum"] = insight.properties.justification.minimum;
                                    row["Maximum"] = insight.properties.justification.maximum;
                                    row["ExpectedValue"] = insight.properties.justification.expectedValue;
                                    row["ExpectedMin"] = insight.properties.justification.expectedMin;
                                    row["ExpectedMax"] = insight.properties.justification.expectedMax;
                                    row["DeltaPercent"] = insight.properties.justification.deltaPercent;

                                    sourceData.Rows.Add(row);  
                                }
                            }
                        }
                    }
                }
                if(sourceData.Rows.Count > 0)
                {
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "CostInsightsData";
                    bcp.WriteToServer(sourceData);
                } 
            }
            catch(Exception ex)
            {
                string errorMessage = ex.Message;
                log.LogError(errorMessage, "An exception occured");
            }
        }
    }
}