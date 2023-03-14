using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction
{

    public class GCPAdvisorModel
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
        //public string state { get; set; }
        //public Ancestors ancestors { get; set; }
        //public List<string> associated_insights { get; set; }
        //public string recommendation_details { get; set; }
        public string priority { get; set; }
    }
    public class Ancestors
    {
        public string organization_id { get; set; }
        public List<string> folder_ids { get; set; }
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


}
