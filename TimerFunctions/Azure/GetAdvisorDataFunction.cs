using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Rest;
using Microsoft.Extensions.Logging;
using Microsoft.Identity.Client;
using Microsoft.Azure.Management.Subscription;
using Newtonsoft.Json;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace Budget.TimerFunction.Azure
{
    public class GetAdvisorDataFunction
    {
        [FunctionName("GetAdvisorDataFunction")]
        public async Task Run([TimerTrigger("%Timer%")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");

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
            sourceData.Columns.Add("RecommendationId");
            sourceData.Columns.Add("Type");
            sourceData.Columns.Add("Name");
            sourceData.Columns.Add("Category");
            sourceData.Columns.Add("Impact");
            sourceData.Columns.Add("ImpactedField");
            sourceData.Columns.Add("ImpactedValue");
            sourceData.Columns.Add("LastUpdated");
            sourceData.Columns.Add("RecommendationTypeId");
            sourceData.Columns.Add("Region");
            sourceData.Columns.Add("ResourceId");
            sourceData.Columns.Add("Problem");
            sourceData.Columns.Add("Solution");
			sourceData.Columns.Add("SavingsAmount");            
			sourceData.Columns.Add("AnnualSavingsAmount");            
			sourceData.Columns.Add("ResourceGroup");
            
            foreach (var subscription in subscriptionClient.Subscriptions.List())
            {
                log.LogInformation("Subscription id is " +subscription.SubscriptionId);
                string subscriptionId = subscription.SubscriptionId;

                var advisorApiUrl = $"https://management.azure.com/subscriptions/{subscriptionId}/providers/Microsoft.Advisor/recommendations?api-version=2020-01-01";
                var advisorApiResult = httpClient.GetAsync(advisorApiUrl).Result;

                if (advisorApiResult.IsSuccessStatusCode)
                {
                    var result = advisorApiResult.Content.ReadAsStringAsync().Result;
                    dynamic advisorApiResponse = JsonConvert.DeserializeObject<AdvisorApiResponse>(result);

                    foreach(Value row in advisorApiResponse.value)
                    {
                        string[] resourceGroup =  row.properties.resourceMetadata.resourceId.Split('/');
                        sourceData.Rows.Add(null, row.id, row.type, row.name, row.properties.Category,
                            row.properties.Impact, row.properties.ImpactedField, row.properties.ImpactedValue,
                            row.properties.LastUpdated, row.properties.RecommendationTypeId,
                            row.properties.extendedProperties == null ? string.Empty : row.properties.extendedProperties.region,
                            row.properties.resourceMetadata.resourceId, row.properties.shortDescription.problem,
                            row.properties.shortDescription.solution,
                            row.properties.extendedProperties == null ? null : row.properties.extendedProperties.SavingsAmount,
                            row.properties.extendedProperties == null ? null : row.properties.extendedProperties.AnnualSavingsAmount,
                            resourceGroup.Length > 5 ? resourceGroup[4].ToUpper() : string.Empty);
                    }
                }                   
            }

            if(sourceData.Rows.Count > 0)
            {
                using (SqlConnection connection = new SqlConnection(myConnectionString))
                {
                    SqlCommand command = new SqlCommand("DELETE FROM AdvisorRecommendations;", connection);
                    command.Connection.Open();
                    command.ExecuteNonQuery();
                    command.Connection.Close();
                }
                SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                bcp.DestinationTableName = "AdvisorRecommendations";
                bcp.WriteToServer(sourceData);
            }       
        }       
    }
}
