using System;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using Budget.TimerFunction;
using CsvHelper;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.IO.Compression;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;
using AzureFunction.Model.Aws;
using Newtonsoft.Json;

namespace Budget.TimerFunction.Aws
{

    public class AWSCloudConsumptionFunction
    {
        [FunctionName("AWSCloudConsumptionFunction")]
        public async Task RunAsync([TimerTrigger("%AwsDailyTimer%")] TimerInfo myTimer, ILogger logger)
        {
            logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption function started.");
            string AccountsConfigJson = ConfigStore.Aws.AWSAccountsAccessKeys;
            JObject obj = JObject.Parse(AccountsConfigJson);
            AwsCloudConsumptionModel awsCloudConsumptionModel = new AwsCloudConsumptionModel();

            string endDate;
            DateTime CurrentDate = DateTime.Today;

            var accountAccessKeys = new AccountAccessKeys();
            foreach (JProperty prop in obj.Properties())
            {
                logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption - AccountId {prop.Name} progress");

                accountAccessKeys = JsonConvert.DeserializeObject<AccountAccessKeys>(prop.Value.ToString());
                awsCloudConsumptionModel.AccountId = prop.Name;
                awsCloudConsumptionModel.AccountAccessKeys = accountAccessKeys;

                //logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption");
                DateTime startDate = new DateTime(CurrentDate.Year, CurrentDate.Month, 01);
                //current date is first day of month then set prefix (1st day of previous month - 1st day of current month) folder path 

                if (CurrentDate == startDate)
                {
                    startDate = startDate.AddMonths(-1);
                    awsCloudConsumptionModel.CreatedOn = startDate.AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                }
                else if (CurrentDate == startDate.AddDays(4))
                {
                    startDate = startDate.AddMonths(-1);
                    endDate = startDate.AddMonths(1).ToString("yyyyMMdd");
                    awsCloudConsumptionModel.Prefix = startDate.ToString("yyyyMMdd") + "-" + endDate;
                    awsCloudConsumptionModel.StartDate = startDate;
                    awsCloudConsumptionModel.EndDate = endDate;
                    awsCloudConsumptionModel.CurrentDate = CurrentDate;
                    awsCloudConsumptionModel.CreatedOn = startDate.AddMonths(1).AddDays(-1).ToString("yyyyMMdd");
                    await PutCloudConsumptionFunction(logger, awsCloudConsumptionModel);
                }
                else
                {
                    awsCloudConsumptionModel.CreatedOn = CurrentDate.ToString("yyyyMMdd");
                }

                endDate = startDate.AddMonths(1).ToString("yyyyMMdd");
                awsCloudConsumptionModel.Prefix = startDate.ToString("yyyyMMdd") + "-" + endDate;
                awsCloudConsumptionModel.StartDate = startDate;
                awsCloudConsumptionModel.EndDate = endDate;
                awsCloudConsumptionModel.CurrentDate = CurrentDate;              
                await PutCloudConsumptionFunction(logger, awsCloudConsumptionModel);

                logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption - AccountId {prop.Name} Completed..");
            }
        }
        public async Task<bool> PutCloudConsumptionFunction(ILogger logger, AwsCloudConsumptionModel awsCloudConsumption)
        {
            try
            {
                AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(awsCloudConsumption.AccountAccessKeys.AccessKey, awsCloudConsumption.AccountAccessKeys.SecretKey), Amazon.RegionEndpoint.USEast1);
                ListObjectsRequest request = new ListObjectsRequest();
                request.BucketName = awsCloudConsumption.AccountAccessKeys.BucketName;

                request.Prefix = string.Format(awsCloudConsumption.AccountAccessKeys.FilePath, awsCloudConsumption.Prefix);

                //request.Prefix = $"{awsCloudConsumption.AccountAccessKeys.FolderPath}/" + awsCloudConsumption.Prefix + "/FinOps_POV-00001.csv.zip";

                DataTable sourceData = new DataTable();
                sourceData.Columns.Add("Id");
                sourceData.Columns.Add("AccountId");
                sourceData.Columns.Add("BillingPeriodEndDate");
                sourceData.Columns.Add("Location");
                sourceData.Columns.Add("RegionCode");
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

                try
                {
                    if (Path.GetExtension(request.Prefix).ToLower() == ".zip")
                    {
                        var response = await s3Client.GetObjectAsync(awsCloudConsumption.AccountAccessKeys.BucketName, request.Prefix);
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
                                    if (dr["lineItem/LineItemType"].ToString().ToLower() == "usage")
                                    {
                                        DataRow row = sourceData.NewRow();                                  
                                        row["AccountId"] = awsCloudConsumption.AccountId;
                                        row["BillingPeriodEndDate"] = dr["bill/BillingPeriodEndDate"];
                                        row["Location"] = dr["product/fromLocation"];
                                        row["RegionCode"] = dr["product/fromRegionCode"];
                                        row["ItemDescription"] = dr["lineItem/LineItemDescription"];
                                        row["ItemType"] = dr["lineItem/LineItemType"];
                                        row["ProductCode"] = dr["lineItem/ProductCode"];
                                        row["ResourceId"] = dr["lineItem/ResourceId"];
                                        row["UsageAccountId"] = dr["lineItem/UsageAccountId"];
                                        row["UsageEndDate"] = dr["lineItem/UsageEndDate"];
                                        row["UsageType"] = dr["lineItem/UsageType"];
                                        row["CurrencyCode"] = dr["lineItem/CurrencyCode"];
                                        row["Cost"] = dr["pricing/publicOnDemandCost"];
                                        row["ProductGroup"] = dr["product/group"];
                                        row["ProductFamily"] = dr["product/productFamily"];
                                        row["ProductName"] = dr["product/ProductName"];
                                        row["ResourceTagsUserEnv"] = dr["resourceTags/user:Env"];
                                        row["ResourceTagsUserName"] = dr["resourceTags/user:Name"];
                                        row["ResourceTagsUserProject"] = dr["resourceTags/user:Project"];
                                        row["CreatedOn"] = DateTime.ParseExact(awsCloudConsumption.CreatedOn, "yyyyMMdd", null).ToString("yyyy-MM-dd"); 
                                        sourceData.Rows.Add(row);
                                    }
                                }
                                logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption - Account {awsCloudConsumption.AccountId} No of rows {sourceData.Rows.Count}");
                            }
                        }
                    }
                    if (sourceData.Rows.Count > 0)
                    {
                        using (SqlConnection sourceConnection = new SqlConnection(ConfigStore.SQLConnectionString))
                        {
                            sourceConnection.Open();
                            logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption - Account {awsCloudConsumption.AccountId} deleting records from  {awsCloudConsumption.StartDate.ToString("yyyy/MM/dd")} to {awsCloudConsumption.CreatedOn}.");
                            //Perform an Delete operation for old data from the source table.
                            SqlCommand commandRowCount = new SqlCommand("Delete from dbo.AWSCloudConsumption_1 where CreatedOn between '" + awsCloudConsumption.StartDate.ToString("yyyy/MM/dd") + "' and '" + awsCloudConsumption.CreatedOn + "' and AccountId='"+ awsCloudConsumption.AccountId+ "';", sourceConnection);
                            commandRowCount.ExecuteNonQuery();
                         
                            //Perform Bulk Operation 
                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSCloudConsumption_1";
                            bcp.WriteToServer(sourceData);
                            logger.Log(LogLevel.Information, "AWSCloudConsumptionFunction", $"AWSCloudConsumption - Account {awsCloudConsumption.AccountId} data inserted.");
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.Log(LogLevel.Error, "AWSCloudConsumptionFunction", $"AWS Cloud Consumption Function Exception : {ex.Message} - {ex.InnerException}");
                    throw;
                }
            }
            catch (Exception ex)
            {
                logger.Log(LogLevel.Error, "AWSCloudConsumptionFunction", $"AWS Cloud Consumption Function Exception : {ex.Message} - {ex.InnerException}");
                throw;
            }
            return true;
        }
    }
}
