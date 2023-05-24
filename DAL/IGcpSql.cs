using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;



namespace Budget.TimerFunction
{
   public interface IGcpSql
    {
        void SaveGcpAdvisor(List<GCPAdvisorModel.GCPAdvisor> objAdvisor);
        void SaveBillingCost(List<GCPBillingCostModel.GCPBillingCost> listGCPDdata);
        void SaveGcpBudget(List<GcpBudgetModel.GcpBudget> objBudgetList, string date);
        void SaveCarbonFootPrint(DataTable dt, string date);
        void SaveGcpTags(List<GcpTagsModel.GcpTags> listGCPTagsdata);
        void SaveGcpUtilization(List<GCPUtilizationModel.GCPUtilization> objUtilization, string date);
        void SaveGcpOrphaned(DataTable dt);
    }
}
