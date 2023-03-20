using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction.GCPAdvisorModel
{
    public class GCPAdvisor
    {
        public string ProjectNumber { get; set; }
        public string Name { get; set; }
        public string Location { get; set; }
        public string Type { get; set; }
        public string SubType { get; set; }
        //public List<string> target_resources { get; set; }
        public string Description { get; set; }
        public string Category { get; set; }
        public string CurrencyCode { get; set; }
        public int? Units { get; set; }
        public int Nanos { get; set; }
        public DateTime LastRefreshDate { get; set; }
        public string Severity { get; set; }
    }
    public class GCPAdvisorRecommendation
    {
        public string cloud_entity_type { get; set; }
        public string cloud_entity_id { get; set; }
        public string name { get; set; }
        public string location { get; set; }
        public string recommender { get; set; }
        public string recommender_subtype { get; set; }
        //public List<string> target_resources { get; set; }
        public string description { get; set; }
        public DateTime last_refresh_time { get; set; }
        public PrimaryImpact primary_impact { get; set; } 
        public string priority { get; set; }
    }
 

    public class Cost
    {
        public string currency_code { get; set; }
        public int? units { get; set; }
        public int nanos { get; set; }
    }

    public class CostProjection
    {
        public Cost cost { get; set; }
        public Duration duration { get; set; }
    }

    public class Duration
    {
        public int seconds { get; set; }
       //public object nanos { get; set; }
    }

    public class PrimaryImpact
    {
        public string category { get; set; }
        public CostProjection cost_projection { get; set; }
    }

    public class GCPAdvisorInsight
    {
        public string cloud_entity_type { get; set; }
        public string cloud_entity_id { get; set; }
        public string name { get; set; }
        public string location { get; set; }
        public string insight_type { get; set; }
        public string insight_subtype { get; set; }
        public string description { get; set; }
        public DateTime last_refresh_time { get; set; }
        public string severity { get; set; }
        public string category { get; set; }
    }

}
