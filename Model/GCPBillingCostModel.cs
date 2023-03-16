using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction.GCPBillingCostModel
{
    public class GCPBillingCost
    {
        public string BillingAccountId { get; set; }
        public string ServiceId { get; set; }
        public string ServiceDesc { get; set; }
        public string SkuId { get; set; }
        public string SkuDesc { get; set; }
        public string ProjectId { get; set; }
        public string ProjectNumber { get; set; }
        public string ProjectName { get; set; }
        public DateTime UsageStartDate { get; set; }
        public DateTime Date { get; set; }
        public DateTime ExportTime { get; set; }
        public string Location { get; set; }
        public string Region { get; set; }
        public string ResourceName { get; set; }
        public string ResourceId { get; set; }
        public decimal Cost { get; set; }
        public decimal CostUsd { get; set; }
        public string Currency { get; set; }
        public double CurrencyConversionRate { get; set; }
    }


}
