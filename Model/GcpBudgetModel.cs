using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction.GcpBudgetModel
{

    public class GcpBudget
    {
        public string budgetDisplayName { get; set; }
        public double costAmount { get; set; }
        public DateTime costIntervalStart { get; set; }
        public double budgetAmount { get; set; }
        public string budgetAmountType { get; set; }
        public string currencyCode { get; set; }
        public decimal alertThresholdExceeded { get; set; }
        public decimal forecastThresholdExceeded { get; set; }
    }


}
