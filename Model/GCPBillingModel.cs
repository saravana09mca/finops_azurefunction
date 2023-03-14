using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction
{
    public class GCPBillingModel
    {
        public string billing_account_id { get; set; }
        public Service service { get; set; }
        public Sku sku { get; set; }
        public DateTime usage_start_time { get; set; }
        public DateTime usage_end_time { get; set; }
        public Project project { get; set; }
        public List<Labels> labels { get; set; }
        public List<SystemLabel> system_labels { get; set; }
        public Location location { get; set; }
        public Resource resource { get; set; }
        public List<Tags> tags { get; set; }
        public DateTime export_time { get; set; }
        public decimal cost { get; set; }
        public decimal CostUsd { get; set; }
        public string currency { get; set; }
        public decimal currency_conversion_rate { get; set; }
        //public Usage usage { get; set; }
        //public List<object> credits { get; set; }
        //public Invoice invoice { get; set; }
        public string cost_type { get; set; }
       // public AdjustmentInfo adjustment_info { get; set; }
    }
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    //public class AdjustmentInfo
    //{
    //    public string id { get; set; }
    //    public string description { get; set; }
    //    public string mode { get; set; }
    //    public string type { get; set; }
    //}

    //public class Ancestor
    //{
    //    public string resource_name { get; set; }
    //    public string display_name { get; set; }
    //}

    //public class Invoice
    //{
    //    public string month { get; set; }
    //}

    public class Location
    {
        public string location { get; set; }
       // public string country { get; set; }
        public string region { get; set; }
       // public string zone { get; set; }
    }

    public class Project
    {
        public string id { get; set; }
       public string number { get; set; }
        public string name { get; set; }
        //public List<object> labels { get; set; }
        //public string ancestry_numbers { get; set; }
        //public List<Ancestor> ancestors { get; set; }
    }

    public class Resource
    {
        public string name { get; set; }
        public string global_name { get; set; }
    }

   

    public class Service
    {
        public string id { get; set; }
        public string description { get; set; }
    }

    public class Sku
    {
        public string id { get; set; }
        public string description { get; set; }
    }

    public class SystemLabel
    {
        public string key { get; set; }
        public string value { get; set; }
    }
    public class Labels
    {
        public string key { get; set; }
        public string value { get; set; }
    }

    public class Tags
    {
        public string key { get; set; }
        public string value { get; set; }
    }


    //public class Usage
    //{
    //    public double amount { get; set; }
    //    public string unit { get; set; }
    //    public double amount_in_pricing_units { get; set; }
    //    public string pricing_unit { get; set; }
    //}


}
