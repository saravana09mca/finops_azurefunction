using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Amazon.S3;
using Amazon.S3.Model;
using System.IO;
using Amazon.Runtime;
using Amazon.S3.Transfer;
using System;
using System.Data;
using System.Text;
using System.Text.RegularExpressions;
using System.IO.Compression;
using CsvHelper;
using System.Globalization;
using System.Data.SqlClient;
using Microsoft.VisualBasic;
using Microsoft.Extensions.Logging;

namespace Budget.TimerFunction
{

    public class AWSCloudConsumptionFunction
    {
        private readonly IAmazonS3 amazonS3;
        public AWSCloudConsumptionFunction(IAmazonS3 amazonS3)
        {
            this.amazonS3 = amazonS3;
        }
        [FunctionName("AWSOrphanedResourcesFunction")]
        public async Task RunAsync([TimerTrigger("%AwsDailyTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSCloudConsumption function executed at: {DateTime.Now}");

            AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new ListObjectsRequest();
            request.BucketName = ConfigStore.Aws.BucketName;
            // request.Prefix = "costreport/FinOps_POV/historicaldata/EYcost-Jan 2022.zip";

            string endDate, Prefix;
            DateTime CurrentDate = DateTime.Today;
            DateTime startDate = new DateTime(CurrentDate.Year, CurrentDate.Month, 01);

            //current date is first day of month then set prefix (1st day of previous month - 1st day of current month) folder path 
            if (CurrentDate == startDate)
            {
                startDate = startDate.AddMonths(-1);
                endDate = startDate.AddMonths(1).ToString("yyyyMMdd");
                Prefix = startDate.ToString("yyyyMMdd") + "-" + endDate;
                request.Prefix = "costreport/FinOps_POV/" + Prefix + "/FinOps_POV-00001.csv.zip";
            }
            //current date is not first day of month then Set prefix of (1st day of Current month - 1st day of Next month) folder path 
            else
            {
                endDate = startDate.AddMonths(1).ToString("yyyyMMdd");
                Prefix = startDate.ToString("yyyyMMdd") + "-" + endDate;
                request.Prefix = "costreport/FinOps_POV/" + Prefix + "/FinOps_POV-00001.csv.zip";
            }

            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("BillingPeriodEndDate");           
            sourceData.Columns.Add("RegionCode");
            sourceData.Columns.Add("Location");
            sourceData.Columns.Add("ItemDescription");
            sourceData.Columns.Add("ItemType");
            sourceData.Columns.Add("ProductCode");
            sourceData.Columns.Add("ResourceId");
            sourceData.Columns.Add("UsageAccountId");
            sourceData.Columns.Add("UsageEndDate");
            sourceData.Columns.Add("UsageType");
            sourceData.Columns.Add("CurrencyCode");
            sourceData.Columns.Add("Cost");
            sourceData.Columns.Add("ProductGroup");
            sourceData.Columns.Add("ProductFamily");
            sourceData.Columns.Add("ProductName");
            sourceData.Columns.Add("ResourceTagsUserEnv");
            sourceData.Columns.Add("ResourceTagsUserName");
            sourceData.Columns.Add("ResourceTagsUserProject");
            sourceData.Columns.Add("CreatedOn");
            //DataTable LatestData = sourceData;
            try
            {
                foreach (S3Object obj in res.S3Objects)
            {
                Console.WriteLine(obj.Key);
                if (Path.GetExtension(obj.Key).ToLower() == ".zip")
                {
                    var response = s3Client.GetObjectAsync(ConfigStore.Aws.BucketName, obj.Key).Result;
                    using var memoryStream = new MemoryStream();
                    response.ResponseStream.CopyTo(memoryStream);
                    // Create a ZipArchive object from the memory stream
                    using (ZipArchive archive = new ZipArchive(memoryStream, ZipArchiveMode.Read))
                    {
                        foreach (ZipArchiveEntry entry in archive.Entries)
                        {
                                // Open a StreamReader for the ZipArchiveEntry
                                using StreamReader reader = new StreamReader(entry.Open());
                                using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                                using var dr = new CsvDataReader(csv);

                                while (dr.Read())
                                {
                                    //select only AWS Usage data from report and exclude tax,fees data
                                    if (dr["lineItem/UsageType"].ToString().ToLower()== "usage")
                                    {
                                        DataRow row = sourceData.NewRow();
                                        row["BillingPeriodEndDate"] = dr["bill/BillingPeriodEndDate"];
                                        row["RegionCode"] = dr["product/fromRegionCode"];
                                        row["Location"] = dr["product/fromLocation"];
                                        row["ItemDescription"] = dr["lineItem/LineItemDescription"];
                                        row["ItemType"] = dr["lineItem/LineItemType"];
                                        row["ProductCode"] = dr["lineItem/ProductCode"];
                                        row["ResourceId"] = dr["lineItem/ResourceId"];
                                        row["UsageAccountId"] = dr["lineItem/UsageAccountId"];
                                        row["UsageEndDate"] = dr["lineItem/UsageEndDate"];
                                        row["UsageType"] = dr["lineItem/UsageType"];
                                        row["CurrencyCode"] =dr["lineItem/CurrencyCode"];
                                        row["Cost"] = dr["pricing/publicOnDemandCost"];
                                        row["ProductGroup"] = dr["product/group"];
                                        row["ProductFamily"] = dr["product/productFamily"];
                                        row["ProductName"] = dr["product/ProductName"];
                                        row["ResourceTagsUserEnv"] = dr["resourceTags/user:Env"];
                                        row["ResourceTagsUserName"] = dr["resourceTags/user:Name"];
                                        row["ResourceTagsUserProject"] = dr["resourceTags/user:Project"];
                                        
                                        //current date is first day of month then set createdOn last day of previous month 
                                        if (CurrentDate == startDate) row["CreatedOn"] = startDate.AddDays(-1);

                                        //set the CreatedOn as Currentdate
                                        else row["CreatedOn"] = CurrentDate;
                                        sourceData.Rows.Add(row);
                                    }
                                }
                         }
                    }
                }
                
            }
                if (sourceData.Rows.Count > 0)
                {
                    using (SqlConnection sourceConnection = new SqlConnection(ConfigStore.SQLConnectionString))
                    {
                        sourceConnection.Open();
                        // Check the count of existing data from the source table
                        SqlCommand commandRowCount = new SqlCommand("select count(*) FROM dbo.AWSCloudConsumption where CreatedOn >=" + startDate + ";", sourceConnection);
                        long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());
                        if (countStart == 0)
                        {

                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSCloudConsumption";
                            bcp.WriteToServer(sourceData);
                        }
                        else
                        {
                            //Perform an Delete operation for old data from the source table.
                            commandRowCount = new SqlCommand("Delete from dbo.AWSCloudConsumption where CreatedOn >=" + startDate + ";", sourceConnection);
                            commandRowCount.ExecuteScalar();
                        }
                    }
                    Console.WriteLine(sourceData);
                }
            }
            catch (Exception Excep)
            {
                Console.WriteLine(Excep.Message, Excep.InnerException);
            }
        }
    }
}
