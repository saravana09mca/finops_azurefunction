using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction.GCPUtilizationModel
{

    public class Label
    {
        public string key { get; set; }
        public string value { get; set; }
    }

    public class PointData
    {
        public TimeInterval timeInterval { get; set; }
        public Values values { get; set; }
    }

    public class PointDescriptor
    {
        public string key { get; set; }
        public string value { get; set; }
    }

    public class GCPUtilizationList
    {
        public string metricName { get; set; }
        public TimeSeriesDescriptor timeSeriesDescriptor { get; set; }
        public PointData pointData { get; set; }
    }
    public class GCPUtilization
    {
        public string MetricName { get; set; }
        public string ProjectId { get; set; }
        public string InstanceId { get; set; }
        public string Date { get; set; }
        public decimal AvgUtilization { get; set; }
        public decimal MaxUtilization { get; set; }
        public decimal MinUtilization { get; set; }
    }

    public class TimeInterval
    {
        public DateTime start_time { get; set; }
        public DateTime end_time { get; set; }
    }

    public class TimeSeriesDescriptor
    {
        public List<Label> labels { get; set; }
        public List<PointDescriptor> pointDescriptors { get; set; }
    }

    public class Values
    {
        public object boolean_value { get; set; }
        public object int64_value { get; set; }
        public decimal double_value { get; set; }
        public object string_value { get; set; }
        public object distribution_value { get; set; }
    }




}
