using System;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using Budget.TimerFunction;
using CsvHelper;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using System.Threading;

namespace Budget.TimerFunction.Aws
{
    public class AWSUtilizationFunction
    {
        [FunctionName("AWSUtilizationFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSUtilization function executed at: {DateTime.Now}");

            AmazonS3Client s3Client = new(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new();
            ListObjectsResponse MoveOldObjReqlist = new();
            bool IsBulkInsertResult = false;
            request.BucketName = ConfigStore.Aws.NewBucketName;
            request.Prefix = "utilizationreports";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("AccountID");
            sourceData.Columns.Add("ServiceCategory");
            sourceData.Columns.Add("InstnceID");
            sourceData.Columns.Add("State");
            sourceData.Columns.Add("ServiceType");
            sourceData.Columns.Add("Region");
            sourceData.Columns.Add("Metric");
            sourceData.Columns.Add("Average");
            sourceData.Columns.Add("Minimum");
            sourceData.Columns.Add("Maximum");
            sourceData.Columns.Add("Timestamp");
            sourceData.Columns.Add("Tags");
            sourceData.Columns.Add("InsertDate");

            List<string> accountIds = new();
            if (!string.IsNullOrEmpty(ConfigStore.Aws.AccountIds))
            {
                accountIds = ConfigStore.Aws.AccountIds.Split(",").ToList();
            }

            try
            {
                foreach (S3Object obj in res.S3Objects)
                {
                    if (!obj.Key.EndsWith("/")) // Check if it's not a folder
                    {
                        if (obj.Size != 0 && obj.Key.Contains('_'))
                        {
                            int accIdIndex = Path.GetFileName(obj.Key).IndexOf('_');
                            //Extract the Data from the CSV file
                            string accountId = Path.GetFileName(obj.Key).Substring(0, accIdIndex);
                            if (accountIds.Contains(accountId))
                            {
                                accountIds.Remove(accountId);
                                var response = s3Client.GetObjectAsync(ConfigStore.Aws.NewBucketName, obj.Key).Result;
                                using StreamReader reader = new StreamReader(response.ResponseStream);
                                using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                                using var dr = new CsvDataReader(csv);
                                int i = 0;
                                while (dr.Read())
                                {
                                    i++;
                                    DataRow row = sourceData.NewRow();
                                    row["AccountID"] = dr["AccountID"];
                                    row["ServiceCategory"] = dr["ServiceCategory"];
                                    row["InstnceID"] = dr["InstnceID"];
                                    row["State"] = dr["State"];
                                    row["ServiceType"] = dr["ServiceType"];
                                    row["Region"] = dr["Region"];
                                    row["Metric"] = dr["Metric"];
                                    row["Average"] = dr["Average"];
                                    row["Minimum"] = dr["Minimum"];
                                    row["Maximum"] = dr["Maximum"];
                                    row["Timestamp"] = dr["Timestamp"];
                                    row["Tags"] = dr["Tags"];
                                    row["InsertDate"] = DateTime.Now;
                                    sourceData.Rows.Add(row);
                                }
                                log.LogInformation($"Account ID {accountId} -  {i} records processed.");
                                MoveOldObjReqlist.S3Objects.Add(obj);
                            }

                        }
                    }
                }
                foreach (var accountId in accountIds)
                {
                    log.LogError($"Error - Account ID {accountId} not available in bucket");
                }

                if (sourceData.Rows.Count > 0)
                {
                    using (SqlConnection sourceConnection = new SqlConnection(ConfigStore.SQLConnectionString))
                    {
                        sourceConnection.Open();

                        // Perform an Delete operation for old data from the source table.
                        SqlCommand commandRowCount = new SqlCommand("Truncate table  " + "dbo.AWSUtilization;", sourceConnection);
                        long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());
                        if (countStart == 0)
                        {
                            //Perform Bulk Insert Opertion to Source table
                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSUtilization";
                            bcp.WriteToServer(sourceData);
                            IsBulkInsertResult = true;
                        }
                        sourceConnection.Close();
                    }
                }
                //After succesful bulk insert move old file from s3 bucket to backup folder 
                if (IsBulkInsertResult == true && MoveOldObjReqlist.S3Objects.Count > 0)
                {
                    DeleteOldBackUpFiles(s3Client, "utilizationreports/backup", ConfigStore.Aws.NewBucketName);
                    CopyingFilesToBackUpFolder(s3Client, "utilizationreports/backup");
                }
            }

            catch (Exception Excep)
            {
                Console.WriteLine(Excep.Message, Excep.InnerException);
            }
        }
        public void DeleteOldBackUpFiles(AmazonS3Client s3Client, string folderName, string bucketName)
        {
            var listRequest = new ListObjectsV2Request
            {
                BucketName = bucketName,
                Prefix = folderName
            };

            ListObjectsV2Response listResponse;
            do
            {
                listResponse = s3Client.ListObjectsV2Async(listRequest).GetAwaiter().GetResult();

                foreach (var file in listResponse.S3Objects)
                {
                    if (!file.Key.EndsWith("/")) // Check if it's not a folder
                    {
                        s3Client.DeleteObjectAsync(bucketName, file.Key).GetAwaiter().GetResult();
                    }
                }

                listRequest.ContinuationToken = listResponse.NextContinuationToken;

            } while (listResponse.IsTruncated);
        }
        public async void CopyingFilesToBackUpFolder(AmazonS3Client s3Client, string DestinationFolder)
        {
            var listRequest = new ListObjectsV2Request
            {
                BucketName = ConfigStore.Aws.NewBucketName,
                Prefix = "utilizationreports" // Only list objects in the specified source folder
            };
            ListObjectsV2Response listResponse;
            do
            {
                listResponse = await s3Client.ListObjectsV2Async(listRequest);
                foreach (var s3Object in listResponse.S3Objects)
                {
                    if (!s3Object.Key.EndsWith("/")) // Check if it's not a folder
                    {
                        string fileName = Path.GetFileName(s3Object.Key);
                        var copyRequest = new CopyObjectRequest
                        {
                            SourceBucket = ConfigStore.Aws.NewBucketName,
                            SourceKey = s3Object.Key,
                            DestinationBucket = ConfigStore.Aws.NewBucketName,
                            DestinationKey = DestinationFolder + "/" + fileName
                        };
                        await s3Client.CopyObjectAsync(copyRequest);
                        var deleteRequest = new DeleteObjectRequest
                        {
                            BucketName = ConfigStore.Aws.NewBucketName,
                            Key = s3Object.Key
                        };
                        await s3Client.DeleteObjectAsync(deleteRequest);
                        Thread.Sleep(2000);
                    }
                }
                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated);
        }

    }
}
