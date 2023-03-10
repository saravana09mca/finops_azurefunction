using System;

namespace Budget.TimerFunction
{
public class RequestBody
    {
        public string metric { get; set; }
        public TimePeriod timePeriod { get; set; }
    }

    public class TimePeriod
    {
        public DateTime start { get; set; }
        public DateTime end { get; set; }
    }
}
