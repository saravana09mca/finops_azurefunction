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
    public class TimerTrigger_ResourceTag_VM
    {
        [FunctionName("TimerTrigger_ResourceTag_VM")]
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

                DataTable sourceData = new DataTable();
                sourceData.Columns.Add("Id");
                sourceData.Columns.Add("SubscriptionID");
                sourceData.Columns.Add("SubscriptionName");
                sourceData.Columns.Add("ResourceGroupName");
                sourceData.Columns.Add("ResourceName");
                sourceData.Columns.Add("ResourceType");
                sourceData.Columns.Add("ResourceId");
                sourceData.Columns.Add("TagKey");
                sourceData.Columns.Add("TagValue");
                sourceData.Columns.Add("IsOrphaned");
                sourceData.Columns.Add("DateAdded");

                var credentials = new TokenCredentials(authResult.AccessToken);

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
                        
                        DataRow row = null;

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
                                        string resourceTagId = resource.id;
                                        row = sourceData.NewRow(); 

                                        //call api to get the virtual machine details
                                        var vmApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Compute/virtualMachines?api-version=2022-11-01&statusOnly=true";
                                        var vmResponse = httpClient.GetAsync(vmApiUrl).Result;
                                        if (vmResponse.IsSuccessStatusCode)
                                        {
                                            var vmResult = vmResponse.Content.ReadAsStringAsync().Result;
                                            dynamic vmJson = JsonConvert.DeserializeObject(vmResult);
                                            Console.WriteLine("{0} \n", vmJson);
                                        

                                            //call api to get the disk details
                                            var diskAoiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Compute/disks?api-version=2021-12-01";
                                            var diskResponse = httpClient.GetAsync(diskAoiUrl).Result;
                                            var diskResult = diskResponse.Content.ReadAsStringAsync().Result;
                                            dynamic diskJson = JsonConvert.DeserializeObject(diskResult);

                                            row = sourceData.NewRow(); 

                                            row["SubscriptionID"] = subscriptionIds;
                                            row["SubscriptionName"] = subscription.DisplayName;
                                            row["ResourceGroupName"] = Convert.ToString(item.name);
                                            row["ResourceName"] = resource.name;
                                            row["ResourceType"] = resource.type;
                                            row["ResourceId"] = resource.id;
                                            row["IsOrphaned"] = false;
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
                                        
                                            foreach (var vm in vmJson.value)
                                            {
                                                string vmId = vm.id;
                                                string status = vm.properties.instanceView.statuses[1].displayStatus;
                                                if(vm.properties.instanceView.statuses[1].displayStatus == "VM deallocated")
                                                {
                                                    row["IsOrphaned"] = true;
                                                }   
                                            }

                                            foreach (var disk in diskJson.value)
                                            {
                                                string diskId = disk.id;
                                                string diskStatus = disk.properties.diskState;
                                                if(diskStatus != "Attached")
                                                {
                                                    row["IsOrphaned"] = true;
                                                }   
                                            }
                                            row["DateAdded"] = DateTime.Now;

                                            sourceData.Rows.Add(row); 
                                        }
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
                        SqlCommand command = new SqlCommand("DELETE FROM ResourceTag_OrphanedVM;", connection);
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "ResourceTag_OrphanedVM";
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