using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureFunction.Model.Aws
{
    public class AwsCloudConsumptionModel
    {

        public string AccountId { get; set; }
        public DateTime StartDate { get; set; }
        public string EndDate { get; set; }
        public string Prefix { get; set; }
        public DateTime CurrentDate { get; set; }
        public string CreatedOn{ get; set; }
       public AccountAccessKeys AccountAccessKeys { get; set; }

    }
    public class AccountAccessKeys
    {
        public string AccessKey { get; set; }
        public string SecretKey { get; set; }
        public string BucketName { get; set; }
        public string FilePath { get; set; }
    }
}
