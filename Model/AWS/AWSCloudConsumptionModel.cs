using System;
using System.Threading.Tasks;

namespace Budget.TimerFunction.AWSCloudConsumptionModel
{
    public class AWSCloudConsumption
    {

        public int Id { get; set; }
        public DateTime BillingPeriodEndDate { get; set; }
        public string Location { get; set; }
        public string RegionCode { get; set; }
        public string ItemDescription { get; set; }
        public string ItemType { get; set; }
        public string ProductCode { get; set; }
        public string ResourceId { get; set; }
        public decimal UsageAccountId { get; set; }
        public DateTime UsageEndDate { get; set; }
        public string UsageType { get; set; }
        public string CurrencyCode { get; set; }
        public decimal Cost { get; set; }
        public string ProductGroup { get; set; }
        public string ProductFamily { get; set; }
        public string ProductName { get; set; }
        public string ResourceTagsUserEnv { get; set; }
        public string ResourceTagsUserName { get; set; }
        public string ResourceTagsUserProject { get; set; }
        public DateTime CreatedOn { get; set; }
    }


}