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
    public class TimerTrigger_OrphanedData
    {
        [FunctionName("TimerTrigger_OrphanedData")]
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
                sourceData.Columns.Add("ResourceGroupName");
                sourceData.Columns.Add("ResourceName");
                sourceData.Columns.Add("ResourceType");
                sourceData.Columns.Add("ResourceId");
                sourceData.Columns.Add("CreationDate");
                sourceData.Columns.Add("IsOrphaned");
                sourceData.Columns.Add("AlertMessage");
                sourceData.Columns.Add("DateAdded");

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    if(subscription.State == SubscriptionState.Enabled)
                    {
                        //call api to get the virtual machine details
                        var vmApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Compute/virtualMachines?api-version=2022-11-01&statusOnly=true";
                        var vmResponse = httpClient.GetAsync(vmApiUrl).Result;
                        if (vmResponse.IsSuccessStatusCode)
                        {
                            var vmResult = vmResponse.Content.ReadAsStringAsync().Result;
                            dynamic vmJson = JsonConvert.DeserializeObject(vmResult);
                            Console.WriteLine("{0} \n", vmJson);

                            foreach (var vm in vmJson.value)
                            {
                                row = sourceData.NewRow(); 
                                string vmId = vm.id;
                                string resourceGroupName = vmId.Split('/')[4];

                                row["SubscriptionID"] = subscriptionIds;
                                row["SubscriptionName"] = subscription.DisplayName;
                                row["ResourceGroupName"] = resourceGroupName;
                                row["ResourceName"] = vm.name;
                                row["ResourceType"] = vm.type;
                                row["ResourceId"] = vmId;
                                row["CreationDate"] = vm.properties.instanceView.statuses[0].time;
                                row["IsOrphaned"] = false;
                                row["AlertMessage"] = "";
                                row["DateAdded"] = DateTime.Now;

                                if(vm.properties.instanceView.statuses[1].displayStatus == "VM deallocated")
                                {
                                    row["IsOrphaned"] = true;
                                    row["AlertMessage"] = $"Virtual Machine which is deallocated with Resource ID - {vmId} is marked as an orphaned resource.";
                                }
                                sourceData.Rows.Add(row); 
                            }
                        }

                        //call api to get the disk details
                        var diskAoiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Compute/disks?api-version=2021-12-01";
                        var diskResponse = httpClient.GetAsync(diskAoiUrl).Result;
                        if (diskResponse.IsSuccessStatusCode)
                        {
                            var diskResult = diskResponse.Content.ReadAsStringAsync().Result;
                            dynamic diskJson = JsonConvert.DeserializeObject(diskResult);
                            foreach (var disk in diskJson.value)
                            {
                                row = sourceData.NewRow(); 
                                string diskId = disk.id;
                                string resourceGroupName = diskId.Split('/')[4];
                                string diskStatus = disk.properties.diskState;

                                row["SubscriptionID"] = subscriptionIds;
                                row["SubscriptionName"] = subscription.DisplayName;
                                row["ResourceGroupName"] = resourceGroupName;
                                row["ResourceName"] = disk.name;
                                row["ResourceType"] = disk.type;
                                row["ResourceId"] = diskId;
                                row["CreationDate"] = disk.properties.timeCreated;
                                row["IsOrphaned"] = false;
                                row["AlertMessage"] = "";
                                row["DateAdded"] = DateTime.Now;

                                if(diskStatus != "Attached")
                                {
                                    row["IsOrphaned"] = true;
                                    row["AlertMessage"] = $"Disk which is unattached with Resource ID - {diskId} is marked as an orphaned resource.";
                                }  
                                sourceData.Rows.Add(row);  
                            }
                        } 

                        //call api to get list of snapshots using subscription ids
                        string snapshotApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.Compute/snapshots?api-version=2021-12-01";
                        var snapshotResponse = httpClient.GetAsync(snapshotApiUrl).Result;

                        if (snapshotResponse.IsSuccessStatusCode)
                        {
                            var result = snapshotResponse.Content.ReadAsStringAsync().Result;
                            dynamic snapshotJson = JsonConvert.DeserializeObject(result);
                            foreach (var item in snapshotJson.value)
                            {
                                DateTime creationDate = item.properties.timeCreated;
                                DateTime todayDate = DateTime.Now;
                                int diff = (todayDate - creationDate).Days;
                                Console.WriteLine("{0} \n", diff);

                                string snapshotId = item.id;
                                string resourceGroupName = snapshotId.Split('/')[4];
                            
                                row = sourceData.NewRow(); 

                                row["SubscriptionID"] = subscriptionIds;
                                row["SubscriptionName"] = subscription.DisplayName;
                                row["ResourceGroupName"] = resourceGroupName;
                                row["ResourceName"] = item.name;
                                row["ResourceType"] = item.type;
                                row["ResourceId"] = snapshotId;
                                row["CreationDate"] = creationDate.ToString();
                                row["IsOrphaned"] = false;
                                row["AlertMessage"] = "";
                                row["DateAdded"] = DateTime.Now;

                                if(diff > 15)
                                {
                                    row["IsOrphaned"] = true;
                                    row["AlertMessage"] = $"Snapshot with Resource ID - {snapshotId} which is older than {diff} days exist, action needed.";
                                }
                                else
                                {
                                    row["IsOrphaned"] = false;
                                }
                                sourceData.Rows.Add(row);  
                            }
                        }

                        //call api to get list of resource groups using subscription ids
                        string resourceApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/resourcegroups?api-version=2021-04-01";
                        var resourceGroupResponse = httpClient.GetAsync(resourceApiUrl).Result;

                        if (resourceGroupResponse.IsSuccessStatusCode)
                        {
                            var result = resourceGroupResponse.Content.ReadAsStringAsync().Result;
                            dynamic resourceGroupJson = JsonConvert.DeserializeObject(result);
                            foreach (var rgname in resourceGroupJson.value)
                            {
                                Console.WriteLine("{0} \n", rgname.name);
                                string rgName = Convert.ToString(rgname.name);
                                log.LogInformation("ResourceGrouptName for Subscription id " +subscription.SubscriptionId + " is " + rgName);
                            
                                //call api to get the Public IP details
                                var publicIPUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/resourceGroups/{rgName}/providers/Microsoft.Network/publicIPAddresses?api-version=2022-09-01";
                                var publicIPResponse = httpClient.GetAsync(publicIPUrl).Result;
                                if (publicIPResponse.IsSuccessStatusCode)
                                {
                                    var ipResult = publicIPResponse.Content.ReadAsStringAsync().Result;
                                    dynamic publicIPJson = JsonConvert.DeserializeObject(ipResult);

                                    foreach (var ip in publicIPJson.value)
                                    {
                                        row = sourceData.NewRow();
                                        string IPId =  ip.id;

                                        row["SubscriptionID"] = subscriptionIds;
                                        row["SubscriptionName"] = subscription.DisplayName;
                                        row["ResourceGroupName"] = rgName;
                                        row["ResourceName"] = ip.name;
                                        row["ResourceType"] = ip.type;
                                        row["ResourceId"] = ip.id;
                                        row["CreationDate"] = "";
                                        row["IsOrphaned"] = false;
                                        row["AlertMessage"] = "";
                                        row["DateAdded"] = DateTime.Now;
                                        if(ip.properties.ContainsKey("ipConfiguration"))
                                        {
                                            row["IsOrphaned"] = false;
                                        }
                                        else
                                        {
                                            row["IsOrphaned"] = true;
                                            row["AlertMessage"] = $"Public IP with Resoure ID - {IPId} is marked as an orphaned resource.";
                                        }
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
                        SqlCommand command = new SqlCommand("Truncate Table OrphanedData;", connection);
                        command.Connection.Open();
                        command.ExecuteNonQuery();
                        command.Connection.Close();
                    }
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    bcp.DestinationTableName = "OrphanedData";
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