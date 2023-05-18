
using System;
using System.Data;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using System.Data.SqlClient;

namespace Budget.TimerFunction.Azure
{
    public class ExportToSql
    {
        [FunctionName("ExportToSql")]
        public void Run([BlobTrigger("des-file-transfer/{name}", Connection = "stfinocostconsumption_STORAGE")]Stream myBlob, string name, ILogger log)
        {
            log.LogInformation($"C# Blob trigger function Processed blob\n Name:{name} \n Size: {myBlob.Length} Bytes");
            var myConnectionString = Environment.GetEnvironmentVariable("sqlconnectionstring");

            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("SubscriptionId");
            sourceData.Columns.Add("SubscriptionName");
            sourceData.Columns.Add("ResourceGroupName");
            sourceData.Columns.Add("CostCenter");
            sourceData.Columns.Add("Date");
            sourceData.Columns.Add("MeterCategory");
            sourceData.Columns.Add("MeterSubCategory");
            sourceData.Columns.Add("BillingCurrency");
            sourceData.Columns.Add("CostInBillingCurrency");
            sourceData.Columns.Add("costInUsd");
            sourceData.Columns.Add("Location");
            sourceData.Columns.Add("ResourceID");
            sourceData.Columns.Add("AdditionalInfo");
            sourceData.Columns.Add("Tag");
            sourceData.Columns.Add("Consumbedservice");
            sourceData.Columns.Add("ServiceFamily");
            sourceData.Columns.Add("DateAdded");
            
            try
            {
                using (var rd = new StreamReader(myBlob))
                {
                    string headerLine = rd.ReadLine();
                    string line;
                    int counter = 0;
                    while ((line = rd.ReadLine()) != null)
                    {
                        counter++;
                        var splits = line.Split(',');
                        //var dt = DateTime.ParseExact(splits[12], "yyyy/MM/dd", new System.Globalization.CultureInfo("en-US"));
                        var dtSplit = splits[12].Split('/','-');
                        var dt = new DateTime(Convert.ToInt16(dtSplit[2]), Convert.ToInt16(dtSplit[0]), Convert.ToInt16(dtSplit[1]));

                        if(dt.Date == DateTime.Now.Date.AddDays(-1))
                        {
                            sourceData.Rows.Add(null, splits[24], splits[25], splits[29], splits[7], dt, 
                            splits[19], splits[20], splits[37], splits[39], splits[40], splits[32],
                            splits[30], splits [45], splits[46], splits[16], splits[13], DateTime.Now);
                        }
                    }
                }
                if(sourceData.Rows.Count > 0)
                {
                    log.LogInformation($"count is " +sourceData.Rows.Count);
                    SqlBulkCopy bcp = new SqlBulkCopy(myConnectionString);
                    log.LogInformation(myConnectionString);
                    bcp.DestinationTableName = "CloudConsumptionTest";
                    bcp.BulkCopyTimeout = 120;
                    bcp.WriteToServer(sourceData);
                }
            }
            catch(Exception ex)
            {
                log.LogError(ex.Message);
            }
        }
    }
}
