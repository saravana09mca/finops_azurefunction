using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.Azure.Management.Automation;
using Microsoft.Azure.Management.Compute;
using System.Net.Http.Headers;
using Microsoft.Azure.Management.Subscription;
using Microsoft.Rest;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.Linq;
using Microsoft.Azure.Management.Subscription.Models;

namespace Budget.TimerFunction.Azure
{
    public class TimerTrigger_StorageCleanUpFunction
    {
        [FunctionName("TimerTrigger_StorageCleanUpFunction")]
        public static async Task Run([TimerTrigger("%TimerCleanup%")]TimerInfo myTimer, ILogger log)
        {
            if (myTimer.IsPastDue)
            {
                log.LogInformation("Timer is running late!");
            }
            log.LogInformation($"C# Timer trigger function triggered at: {DateTime.Now}");
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

                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionIds = subscription.SubscriptionId;
                    if(subscription.State == SubscriptionState.Enabled)
                    {
                        //call api to get export details using subscription ids
                        string exportApiUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/providers/Microsoft.CostManagement/exports?api-version=2022-10-01";
                        var reportResponse = httpClient.GetAsync(exportApiUrl).Result;
                                
                        if (reportResponse.IsSuccessStatusCode)
                        {
                            var reportResult = reportResponse.Content.ReadAsStringAsync().Result;
                            dynamic reportJson = JsonConvert.DeserializeObject(reportResult);
                            foreach (var report in reportJson.value)
                            {
                                string resourceDataUrl = report.properties.deliveryInfo.destination.resourceId;
                                string sourceContainer = report.properties.deliveryInfo.destination.container;
                                string resourceGroupName = resourceDataUrl.Split('/')[4];
                                string accountName = resourceDataUrl.Split('/').Last();

                                //call api to get the account keys of storage account
                                var storageKeyUrl = $"https://management.azure.com/subscriptions/{subscriptionIds}/resourceGroups/{resourceGroupName}/providers/Microsoft.Storage/storageAccounts/{accountName}/listKeys?api-version=2022-09-01";
                                var storageKey = httpClient.PostAsync(storageKeyUrl, null).Result;
                                if (storageKey.IsSuccessStatusCode)
                                {
                                    var storageKeyResult = storageKey.Content.ReadAsStringAsync().Result;
                                    dynamic keyJson = JsonConvert.DeserializeObject(storageKeyResult);
                                    string accountKey = null;
                            
                                    foreach (var key in keyJson.keys)
                                    {
                                        accountKey = key.value;
                                    }

                                    string sourceConnString = "DefaultEndpointsProtocol=https;AccountName=" + accountName + ";AccountKey=" + accountKey + ";EndpointSuffix=core.windows.net";
                                    string blobContainer = report.properties.deliveryInfo.destination.container;
                                    CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse(sourceConnString);
                                
                                    // Create the source blob client.
                                    CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
                                    CloudBlobContainer srcContainer = blobClient.GetContainerReference(blobContainer);
                                    CloudBlobDirectory dirb = srcContainer.GetDirectoryReference(blobContainer);
                                    BlobResultSegment resultSegment = await srcContainer.ListBlobsSegmentedAsync(string.Empty, true, BlobListingDetails.Metadata, 500, null, null, null);

                                    foreach (var blobItem in resultSegment.Results)
                                    {
                                        var blob = (CloudBlob)blobItem;
                                        Console.WriteLine(blob.Name);
                                        string sourceBlobName = blob.Name;
                                        string fileName = sourceBlobName.Split('/').Last();
                                        CloudBlob sourcrBlob = srcContainer.GetBlockBlobReference(sourceBlobName);
                                        var sourceBlobClient = new BlobClient(sourceConnString, sourceContainer, sourceBlobName);

                                        try
                                        {
                                                //Delete blob from source storage account
                                                await sourceBlobClient.DeleteAsync();
                                                log.LogInformation("The blob deleted successfully from the source. BlobName is " + fileName);
                                        }
                                        catch(Exception e)
                                        {
                                            log.LogError("\nThe transfer is canceled: {0}", e.Message);  
                                        }
                                    }
                                }
                            }    
                        }   
                        else
                        {
                            log.LogError("An error occured, Please check if the Export is created for Subscription" + subscription.SubscriptionId);
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
