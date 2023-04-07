using Budget.TimerFunction;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


public interface IGcpCredentials
{
    BigQueryClient GcpAuthentication();
}

