using System;
using System.Collections.Generic;

namespace Budget.TimerFunction
{
    // Root myDeserializedClass = JsonConvert.DeserializeObject<Root>(myJsonResponse);
    public class Datum
    {
        public DateTime timeStamp { get; set; }
        public double average { get; set; }
    }

    public class Name
    {
        public string value { get; set; }
        public string localizedValue { get; set; }
    }

    public class Utilization
    {
        public int cost { get; set; }
        public string timespan { get; set; }
        public string interval { get; set; }
        public List<Values> value { get; set; }
        public string @namespace { get; set; }
        public string resourceregion { get; set; }
    }

    public class Timeseries
    {
        public List<object> metadatavalues { get; set; }
        public List<Datum> data { get; set; }
    }

    public class Values
    {
        public string id { get; set; }
        public string type { get; set; }
        public Name name { get; set; }
        public string displayDescription { get; set; }
        public string unit { get; set; }
        public List<Timeseries> timeseries { get; set; }
        public string errorCode { get; set; }
    }
}