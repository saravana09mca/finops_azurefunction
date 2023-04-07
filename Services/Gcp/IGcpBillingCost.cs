using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunction.Services.Gcp
{
  public interface IGcpBillingCost
    {
        void PutGcpBillingCost(BigQueryClient client);
    }
}
