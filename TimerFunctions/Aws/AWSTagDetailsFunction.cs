using System;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using System.Data;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using Amazon.Auth.AccessControlPolicy;
using CsvHelper;
using System.Data.SqlClient;
using System.Globalization;
using System.IO.Compression;
using System.IO;
using System.Collections.Generic;
using System.Collections;
using System.Linq;

namespace Budget.TimerFunction.Aws
{
    public class AWSTagDetailsFunction
    {
        [FunctionName("AWSTagDetailsFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSTagDetails function executed at: {DateTime.Now}");
            AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new ListObjectsRequest();
            ListObjectsResponse MoveOldObjReqlist = new();
            string DestinationFolders = "tagcomplaince/backup";
            bool IsBulkInsertResult = false;
            List<string> accountIds = new();
            if (!string.IsNullOrEmpty(ConfigStore.Aws.AccountIds))
            {
                accountIds = ConfigStore.Aws.AccountIds.Split(",").ToList();
            }

            request.BucketName = ConfigStore.Aws.NewBucketName;
            request.Prefix = "tagcomplaince";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("AccountID");
            sourceData.Columns.Add("ServiceCategory");
            sourceData.Columns.Add("ServiceName");
            sourceData.Columns.Add("ResourceID");
            sourceData.Columns.Add("Region");
            sourceData.Columns.Add("Tags");
            sourceData.Columns.Add("CreateOn");
            try
            {
                foreach (S3Object obj in res.S3Objects.Distinct().OrderBy(a => a.LastModified))
                {
                    if (!obj.Key.EndsWith("/")) // Check if it's not a folder
                    {
                        if (obj.Size != 0)
                        {
                            string[] newString = obj.Key.Split(new string[] { "_" }, StringSplitOptions.RemoveEmptyEntries);
                            string AccountId = newString[0].Substring(newString[0].IndexOf('/') + 1);
                            //Add Latest object to Latest Object List
                            if (accountIds.Contains(AccountId))
                            {
                                accountIds.Remove(AccountId);
                                MoveOldObjReqlist.S3Objects.Add(obj);
                                //Extract the Data from the CSV file
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
                                    row["ServiceName"] = dr["ServiceName"];
                                    row["ResourceID"] = dr["ResourceID"];
                                    row["Region"] = dr["region"];
                                    row["Tags"] = dr["Tags"];
                                    row["CreateOn"] = DateTime.UtcNow;
                                    sourceData.Rows.Add(row);
                                }
                                log.LogInformation($"Account ID {AccountId} -  {i} records processed.");
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
                        SqlCommand commandRowCount = new SqlCommand("Truncate table  " + "dbo.AWSTagDetails;", sourceConnection);
                        long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());
                        if (countStart == 0)
                        {
                            //Perform Bulk Insert Opertion to Source table
                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSTagDetails";
                            bcp.WriteToServer(sourceData);
                            IsBulkInsertResult = true;
                        }
                        sourceConnection.Close();
                    }
                }
                //After succesful bulk insert delete old files from s3 bucket
                //After succesful bulk insert move old file from s3 bucket to backup folder 
                if (IsBulkInsertResult == true && MoveOldObjReqlist.S3Objects.Count > 0)
                {
                    //Delete Old backup files
                    DeleteOldBackUpFiles(s3Client, DestinationFolders, ConfigStore.Aws.NewBucketName);
                    //Add New backups files
                    CopyingFilesToBackUpFolder(s3Client, DestinationFolders);
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
                Prefix = "tagcomplaince" // Only list objects in the specified source folder
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
                        s3Client.CopyObjectAsync(copyRequest).GetAwaiter();
                        var deleteRequest = new DeleteObjectRequest
                        {
                            BucketName = ConfigStore.Aws.NewBucketName,
                            Key = s3Object.Key
                        };
                        s3Client.DeleteObjectAsync(deleteRequest).GetAwaiter();
                    }
                }
                listRequest.ContinuationToken = listResponse.NextContinuationToken;
            } while (listResponse.IsTruncated);
        }
    }
}
