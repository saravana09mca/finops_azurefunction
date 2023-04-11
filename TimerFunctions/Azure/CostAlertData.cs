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
    public class CostAlertData
    {
        [FunctionName("CostAlertData")]
        public static async Task Run([TimerTrigger("%Timer%")]TimerInfo myTimer, ILogger log)
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
                sourceData.Columns.Add("ResourceId");
                sourceData.Columns.Add("ResourceName");
                sourceData.Columns.Add("ResourceType");
                sourceData.Columns.Add("DefinitionType");
                sourceData.Columns.Add("Category");
                sourceData.Columns.Add("Criteria");
                sourceData.Columns.Add("Threshold");
                sourceData.Columns.Add("Operator");
                sourceData.Columns.Add("Amount");
                sourceData.Columns.Add("Unit");
                sourceData.Columns.Add("CostEntityId");
                sourceData.Columns.Add("CurrentSpend");
                sourceData.Columns.Add("Status");
                sourceData.Columns.Add("AlertMessage");
                sourceData.Columns.Add("AlertCreationTime");
                sourceData.Columns.Add("DateAdded");

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    if(subscription.State == SubscriptionState.Enabled)
                    {
                        //call api to get the CostAlerts
                        var alertApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.CostManagement/alerts?api-version=2022-10-01";
                        var alertJson = httpClient.GetAsync(alertApiUrl).Result;
                        if (alertJson != null && alertJson.IsSuccessStatusCode == true)
                        {
                            var alertResult = alertJson.Content.ReadAsStringAsync().Result;
                            dynamic alertResponse = JsonConvert.DeserializeObject(alertResult);
                            Console.WriteLine("{0} \n", alertResponse);

                            foreach (var alert in alertResponse.value)
                            {
                                string BudgetName = alert.properties.costEntityId;
                                decimal BudgetAmount = alert.properties.details.amount;
                                decimal thresholdPercent = alert.properties.details.threshold*100;

                                row = sourceData.NewRow(); 

                                row["SubscriptionID"] = subscriptionIds;
                                row["SubscriptionName"] = subscription.DisplayName;
                                row["ResourceId"] = alert.id;
                                row["ResourceName"] = alert.name;
                                row["ResourceType"] = alert.type;
                                row["DefinitionType"] = alert.properties.definition.type;
                                row["Category"] = alert.properties.definition.category;
                                row["Criteria"] = alert.properties.definition.criteria;
                                row["Threshold"] = alert.properties.details.threshold;
                                row["Operator"] = alert.properties.details.@operator;
                                row["Amount"] = alert.properties.details.amount;
                                row["Unit"] = alert.properties.details.unit;
                                row["CostEntityId"] = alert.properties.costEntityId;
                                row["CurrentSpend"] = alert.properties.details.currentSpend;
                                row["Status"] = alert.properties.status;
                                if(alert.properties.definition.type == "BudgetForecast")
                                {
                                    if(alert.properties.details.currentSpend > alert.properties.details.amount)
                                    {
                                        row["AlertMessage"] = $"{BudgetName} - Forecasted spend crossed the budget amount of {BudgetAmount}";
                                    }
                                    else
                                    {
                                        row["AlertMessage"] = $"Forecasted spend is {thresholdPercent}% of the budget amount {BudgetAmount}";
                                    }
                                }
                                if(alert.properties.definition.type == "Budget")
                                {
                                    if(alert.properties.details.currentSpend > alert.properties.details.amount)
                                    {
                                        row["AlertMessage"] = $"{BudgetName} - Actual spend crossed the budget amount of {BudgetAmount}";
                                    }
                                    else
                                    {
                                        row["AlertMessage"] = $"Actual spend is {thresholdPercent}% of the budget amount {BudgetAmount}";
                                    }
                                }
                                row["AlertCreationTime"] = alert.properties.creationTime;
                                row["DateAdded"] = DateTime.Now;

                                sourceData.Rows.Add(row);  
                            }
                        }
                    }
                }
                if(sourceData.Rows.Count > 0)
                {
                    using (SqlConnection connection = new SqlConnection(myConnectionString))
                    {
                        SqlCommand command = new SqlCommand("Truncate table CostAlertData;", connection);
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "CostAlertData";
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