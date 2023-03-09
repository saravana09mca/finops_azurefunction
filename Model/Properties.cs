using System;
using System.Collections.Generic;

namespace Budget.TimerFunction
{
    public class Properties
    {
        public string Category { get; set; }
        public string Impact { get; set; }
        public string ImpactedField { get; set; }
        public string ImpactedValue { get; set; }
        public DateTime LastUpdated { get; set; }
        public string RecommendationTypeId { get; set; }
        public ShortDescription shortDescription { get; set; }
        public ExtendedProperties extendedProperties { get; set; }
        public ResourceMetadata resourceMetadata { get; set; }
    }

    public class ExtendedProperties
    {
        public string region { get; set; }
        public int SavingsAmount { get; set; }
        public int AnnualSavingsAmount { get; set; }
    }

    public class ResourceMetadata
    {
        public string resourceId { get; set; }
    }

    public class AdvisorApiResponse
    {
        public List<Value> value { get; set; }
    }

    public class ShortDescription
    {
        public string problem { get; set; }
        public string solution { get; set; }
    }

    public class Value
    {
        public Properties properties { get; set; }
        public string id { get; set; }
        public string type { get; set; }
        public string name { get; set; }
    }
}