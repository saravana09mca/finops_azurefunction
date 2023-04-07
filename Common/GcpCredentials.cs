using Budget.TimerFunction;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunction.Common
{
    public class GcpCredentials:IGcpCredentials
    {
        public GcpCredentials() { }

        public BigQueryClient GcpAuthentication() {
            GoogleCredential credentials = null;
            using (var stream = Helper.GetBlobMemoryStream(ConfigStore.AzureStorageAccountConnectionString, ConfigStore.GCP.GCP_BlobContrainerName, ConfigStore.GCP.GCP_BlobFileName))
            {
                credentials = GoogleCredential.FromStream(stream);
            }
            var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);
            return client;
        }
        //public BigQueryClient GcpCredetials(string filePath)
        //{
        //    //GoogleCredential credentials = null;
        //    GoogleCredential credentials = GoogleCredential.FromFile(filePath);
        //    var client = BigQueryClient.Create(ConfigStore.GCP.GCP_ProjectId, credentials);
        //    return client;
        //}
    }
}
