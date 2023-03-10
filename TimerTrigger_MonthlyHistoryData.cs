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
using System.Threading;
using System.Diagnostics;
using Microsoft.Azure.Storage.DataMovement;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage;
using System.Text;

namespace Budget.TimerFunction
{
    public class TimerTrigger_MonthlyHistoryData
    {
        [FunctionName("TimerTrigger_MonthlyHistoryData")]
        public static async Task Run([TimerTrigger("%TimerHistory%")]TimerInfo myTimer, ILogger log)
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
                
                var today = DateTime.Today;
                var month = new DateTime(today.Year, today.Month, 1);       
                var firstDate = month.AddMonths(-1);
                var lastDate = month.AddDays(-1);

                var requestBody = new RequestBody
                {
                    metric = "ActualCost",
                    timePeriod = new TimePeriod
                    {
                    start = firstDate,
                    end = lastDate
                    }
                };

                var credentials = new TokenCredentials(authResult.AccessToken);
                var subscriptionClient = new SubscriptionClient(credentials);
                foreach (var subscription in subscriptionClient.Subscriptions.List())
                {
                    log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                    string subscriptionId = subscription.SubscriptionId;
                    string subscriptionName = subscription.DisplayName;

                    // serialize to JSON string for POST request body
                    var myRequestJsonBody = JsonConvert.SerializeObject(requestBody);
                    var requestContent = new StringContent(myRequestJsonBody, Encoding.UTF8, "application/json");

                    string historicUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.CostManagement/generateCostDetailsReport?api-version=2022-10-01";

                    // make the POST request
                    var response = await httpClient.PostAsync(historicUrl, requestContent);
                    var responseLocation = response.Headers.Location;
                    string requestLocation = responseLocation.AbsoluteUri;
                    var requestStatus = httpClient.GetAsync(requestLocation).Result;
                    if (requestStatus.IsSuccessStatusCode)
                    {
                        var resultsJson = requestStatus.Content.ReadAsStringAsync().Result;
                        dynamic resultURL = JsonConvert.DeserializeObject(resultsJson);
                        foreach (var resultLink in resultURL.manifest.blobs)
                        {
                            Console.WriteLine("{0} \n", resultLink.blobLink);
                            string blobLink = resultLink.blobLink;
                            Uri uri = new Uri(blobLink);

                            CloudStorageAccount desCloudStorageAccountHistory = CloudStorageAccount.Parse(ConfigStore.DestinationStorageConn);
                            CloudBlobClient descBlobClientHistory = desCloudStorageAccountHistory.CreateCloudBlobClient();
                            string desBlobContainerHistory = ConfigStore.DestinationContainerHistory;
                            string destBlobNameHistory = ConfigStore.DestinationBlobNameHistory + $"{subscriptionName}" + "_" + Guid.NewGuid();
                            CloudBlobContainer desContainerHistory = descBlobClientHistory.GetContainerReference(desBlobContainerHistory); 
                            CloudBlob destinationBlobHistory = desContainerHistory.GetBlockBlobReference(destBlobNameHistory);

                            TransferCheckpoint checkpoint = null;
                            SingleTransferContext context = GetSingleTransferContext(checkpoint); 
                            CancellationTokenSource cancellationSource = new CancellationTokenSource();
                            Stopwatch stopWatch = Stopwatch.StartNew();
                            Task task;
                            try
                            {
                                task = TransferManager.CopyAsync(uri, destinationBlobHistory, true , null, context, cancellationSource.Token);
                                await task;
                                if(task.IsCompleted)
                                {
                                    log.LogInformation("The file copied successfully from the source to destination.");
                                 }
                            }
                            catch(Exception e)
                            {
                                log.LogInformation("\nThe transfer is canceled: {0}", e.Message);  
                            }
                                        
                            stopWatch.Stop();
                            log.LogInformation("\nTransfer operation completed in " + stopWatch.Elapsed.TotalSeconds + " seconds.");
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
        public static SingleTransferContext GetSingleTransferContext(TransferCheckpoint checkpoint)
        {
            SingleTransferContext context = new SingleTransferContext(checkpoint);
            context.ProgressHandler = new Progress<TransferStatus>((progress) =>
            {
                Console.Write("\rBytes transferred: {0}", progress.BytesTransferred );
            });
            
            return context;
        }
    }
}
