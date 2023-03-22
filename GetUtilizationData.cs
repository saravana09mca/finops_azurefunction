using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.Azure.Management.Subscription;
using Newtonsoft.Json;
using Microsoft.Rest;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Budget.TimerFunction
{
    public class GetUtilizationData
    {
        [FunctionName("GetUtilizationData")]
        public async Task Run([TimerTrigger("%Timer%")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");
            DateTime startDate = DateTime.UtcNow.Date.AddDays(-1); //Get previous day start time
            DateTime endDate = DateTime.UtcNow.Date.AddTicks(-1); //Get previous day end time
            string strStartDate = startDate.ToString("O");
            string strEndDate = endDate.ToString("O");

            //MSAL Authentication
            string[] respurceUrl = new string[] {"https://management.azure.com/.default"};
            var clientApp = ConfidentialClientApplicationBuilder
            .Create(clientId: ConfigStore.AADClientId)
            .WithClientSecret(clientSecret: ConfigStore.AADClientSecret)
            .WithAuthority(new Uri (ConfigStore.AADAuthority))
            .Build();

            AuthenticationResult authResult = await clientApp.AcquireTokenForClient(respurceUrl).ExecuteAsync();
            var httpClient = new HttpClient();
            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", authResult.AccessToken);
            
            var credentials = new TokenCredentials(authResult.AccessToken);
            var subscriptionClient = new SubscriptionClient(credentials);

            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");         
            sourceData.Columns.Add("SubscriptionId");
            sourceData.Columns.Add("ResourceGroup");
            sourceData.Columns.Add("VmName");   
            sourceData.Columns.Add("Cost");
            sourceData.Columns.Add("ResourceRegion");
            sourceData.Columns.Add("Date");
            sourceData.Columns.Add("MetricName");
            sourceData.Columns.Add("AverageUtilization");
            sourceData.Columns.Add("MinimumUtilization");
            sourceData.Columns.Add("MaximumUtilization");

            string subscriptionId = string.Empty;
            string resourceGroup = string.Empty;
            string vmName = string.Empty;

            foreach (var subscription in subscriptionClient.Subscriptions.List())
            {
                log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                subscriptionId = subscription.SubscriptionId;
               
                //call api to get list of resource groups using subscription ids
                string resourceApiUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/resourcegroups?api-version=2021-04-01";
                var resourceGroupResponse = httpClient.GetAsync(resourceApiUrl).Result;  
                if (resourceGroupResponse.IsSuccessStatusCode)
                {
                    var result = resourceGroupResponse.Content.ReadAsStringAsync().Result;                
                    dynamic resourceGroupJson = JsonConvert.DeserializeObject(result);
                    foreach (var item in resourceGroupJson.value)
                    {
                        resourceGroup = Convert.ToString(item.name);
                        log.LogInformation("ResourceGrouptName for Subscription id " + subscriptionId + " is " + resourceGroup);

                        if(resourceGroup != null)
                        {
                            //call api to get the virtual machine details
                            var vmApiUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.Compute/virtualMachines?api-version=2022-11-01";
                            var vmResponse = httpClient.GetAsync(vmApiUrl).Result;

                            if (vmResponse.IsSuccessStatusCode)
                            {
                                var vmResult = vmResponse.Content.ReadAsStringAsync().Result;
                                dynamic vmJson = JsonConvert.DeserializeObject(vmResult);

                                foreach (var vm in vmJson.value)
                                {
                                    vmName = vm.name;
                                    log.LogInformation("VM {0} in ResourceGrouptName for Subscription id " + subscriptionId + " is " + resourceGroup, vmName);
                                    //call api to get the virtual machine utilization
                                    var vmUtilizationUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/resourceGroups/{resourceGroup}/providers/Microsoft.Compute/virtualMachines/{vmName}/providers/microsoft.insights/metrics?api-version=2018-01-01&metricnames=Percentage%20CPU&timespan={strStartDate}/{strEndDate}";
                                    var utilizationResponse = httpClient.GetAsync(vmUtilizationUrl).Result;

                                    if (utilizationResponse.IsSuccessStatusCode)
                                    {
                                        var utilizationResult = utilizationResponse.Content.ReadAsStringAsync().Result;
                                        var utilization = JsonConvert.DeserializeObject<Utilization>(utilizationResult);
                                        var currentDate = startDate;

                                        while(currentDate <= endDate && utilization.value[0].timeseries.Count > 0)
                                        {
                                            var vmAvgUtil = utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Count() == 0 ? 0 : utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Average(x => x.average);
                                            var vmMinUtil = utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Count() == 0 ? 0 : utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Min(x => x.average);
                                            var vmMaxUtil = utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Count() == 0 ? 0 : utilization.value[0].timeseries[0].data.Where(y => y.timeStamp.Date.ToShortDateString() ==  currentDate.ToShortDateString()).Max(x => x.average);
                                            sourceData.Rows.Add(null, subscriptionId, resourceGroup,
                                            vmName, utilization.cost, utilization.resourceregion,
                                            currentDate, utilization.value[0].name.value, vmAvgUtil, vmMinUtil, vmMaxUtil);
                                            currentDate = currentDate.AddDays(1);
                                        }                              
                                    }
                                }
                            }                           
                        }
                    }
                }
            }

            if(sourceData.Rows.Count > 0)
            {
                SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                bcp.DestinationTableName = "VmUtilization";
                bcp.WriteToServer(sourceData);
            }
        }
    }
}

