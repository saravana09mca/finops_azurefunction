using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
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

namespace Budget.TimerFunction
{
    public class TimerTrigger_BudgetData
    {
        [FunctionName("TimerTrigger_BudgetData")]
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
                DataTable dt = new DataTable();

                var credentials = new TokenCredentials(authResult.AccessToken);

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    if(subscription.State == SubscriptionState.Enabled)
                    {
                        //call api to get the budget data by passing subscription id 
                        var budgetApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Consumption/budgets?api-version=2021-10-01";
                        var budgetapi = httpClient.GetAsync(budgetApiUrl).Result;
                        if (budgetapi.IsSuccessStatusCode)
                        {
                            var result = budgetapi.Content.ReadAsStringAsync().Result;
                            dynamic budgetResponse = JsonConvert.DeserializeObject(result);
                            Console.WriteLine("{0} \n", budgetResponse);
                        
                            DataTable sourceData = new DataTable();
                            DataRow row = null;

                            sourceData.Columns.Add("Id").DefaultValue = null;
                            sourceData.Columns.Add("SubscriptionID");
                            sourceData.Columns.Add("SubscriptionName");   
                            sourceData.Columns.Add("BudgetId").DefaultValue=string.Empty;
                            sourceData.Columns.Add("BudgetName").DefaultValue=string.Empty;
                            sourceData.Columns.Add("Type").DefaultValue=string.Empty;
                            sourceData.Columns.Add("StartDate").DefaultValue = null;
                            sourceData.Columns.Add("EndDate").DefaultValue = null;
                            sourceData.Columns.Add("TimeGrain").DefaultValue=string.Empty;
                            sourceData.Columns.Add("Budget").DefaultValue=string.Empty;
                            sourceData.Columns.Add("CurrentSpend").DefaultValue=string.Empty;
                            sourceData.Columns.Add("ForecastSpend").DefaultValue=string.Empty;
                            sourceData.Columns.Add("DateAdded").DefaultValue = null;

                            foreach (var budget in budgetResponse.value)
                            {
                                row = sourceData.NewRow();
                            
                                row["SubscriptionID"] = subscriptionIds;
                                row["SubscriptionName"] = subscription.DisplayName;
                                row["BudgetId"] = budget.id;
                                row["BudgetName"] = budget.name;
                                row["Type"] = budget.type;
                                string srcDate = budget.properties.timePeriod.startDate;
                                string endDates = budget.properties.timePeriod.endDate;
                                string subsrc = srcDate.Substring(0,10);
                                string subend = endDates.Substring(0,10);
                                DateTime StartDate = DateTime.ParseExact(subsrc, "dd/MM/yyyy", CultureInfo.CurrentCulture);
                                DateTime EndDate = DateTime.ParseExact(subend, "MM/dd/yyyy", CultureInfo.CurrentCulture);
                                row["StartDate"] =  StartDate;
                                row["EndDate"] = EndDate;
                                row["TimeGrain"] = budget.properties.timeGrain;
                                row["Budget"] = budget.properties.amount;
                                row["CurrentSpend"] = budget.properties.currentSpend.amount;
                                if(budget.properties.ContainsKey("forecastSpend"))
                                {
                                    row["ForecastSpend"] = budget.properties.forecastSpend.amount;
                                }
                                else
                                {
                                    row["ForecastSpend"] = 0;
                                }
                                row["DateAdded"] = DateTime.Now;

                                sourceData.Rows.Add(row);
                            }

                            if(sourceData.Rows.Count > 0)
                            {
                                SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                                bcp.DestinationTableName = "BudgetData";
                                bcp.WriteToServer(sourceData);
                            }
                        }
                    }
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