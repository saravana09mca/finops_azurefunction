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

namespace Budget.TimerFunction
{
    public class TimerTrigger_ResourceTag
    {
        [FunctionName("TimerTrigger_ResourceTag")]
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
                sourceData.Columns.Add("ResourceGroupName");
                sourceData.Columns.Add("ResourceName");
                sourceData.Columns.Add("ResourceType");
                sourceData.Columns.Add("ResourceId");
                sourceData.Columns.Add("TagKey");
                sourceData.Columns.Add("TagValue");
                sourceData.Columns.Add("DateAdded");

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    if(subscription.State == SubscriptionState.Enabled)
                    { 
                        //call api to get list of resource groups using subscription ids
                        string resourceApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/resourcegroups?api-version=2021-04-01";
                        var resourceGroupResponse = httpClient.GetAsync(resourceApiUrl).Result;

                        if (resourceGroupResponse.IsSuccessStatusCode)
                        {
                            var result = resourceGroupResponse.Content.ReadAsStringAsync().Result;
                            dynamic resourceGroupJson = JsonConvert.DeserializeObject(result);
                            foreach (var item in resourceGroupJson.value)
                            {
                                Console.WriteLine("{0} \n", item.name);
                                string rgName = Convert.ToString(item.name);
                                log.LogInformation("ResourceGrouptName for Subscription id " +subscription.SubscriptionId + " is " + rgName);
                            
                                //call api to get the resource details
                                var resourcesApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/resourceGroups/{rgName}/resources?api-version=2021-04-01";
                                var resourceResponse = httpClient.GetAsync(resourcesApiUrl).Result;

                                if (resourceResponse.IsSuccessStatusCode)
                                {
                                    var resourceResult = resourceResponse.Content.ReadAsStringAsync().Result;
                                    dynamic resourceJson = JsonConvert.DeserializeObject(resourceResult);

                                    foreach (var resource in resourceJson.value)
                                    {
                                        row = sourceData.NewRow(); 

                                        row["SubscriptionID"] = subscriptionIds;
                                        row["SubscriptionName"] = subscription.DisplayName;
                                        row["ResourceGroupName"] = rgName;
                                        row["ResourceName"] = resource.name;
                                        row["ResourceType"] = resource.type;
                                        row["ResourceId"] = resource.id;

                                        if(resource.ContainsKey("tags"))
                                        {
                                            foreach (var property in resource.tags)
                                            {
                                                row["TagValue"] = property.Value.ToString();
                                                row["TagKey"] = property.Name;
                                            }
                                        }
                                        else
                                        {
                                            row["TagValue"] = string.Empty;
                                            row["TagKey"] = string.Empty;
                                        }
                                        row["DateAdded"] = DateTime.Now;
                                    
                                        sourceData.Rows.Add(row); 
                                    }  
                                }
                            }
                        }
                    }
                }
                if(sourceData.Rows.Count > 0)
                    {
                        using (SqlConnection connection = new SqlConnection(myConnectionString))
                        {
                            SqlCommand command = new SqlCommand("DELETE FROM ResourceTag;", connection);
                            command.Connection.Open();
                            command.ExecuteNonQuery();
                            command.Connection.Close();
                        }
                        SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                        bcp.DestinationTableName = "ResourceTag";
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