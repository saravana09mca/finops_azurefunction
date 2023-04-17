using System;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using CsvHelper;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Drawing;
using System.Linq;
using Grpc.Core;
using System.Collections.Generic;
using Microsoft.Extensions.FileProviders;
using System.Net.Sockets;
using Microsoft.Identity.Client;
using Azure.Storage.Blobs.Models;

namespace Budget.TimerFunction.Aws
{
    public class AWSOrphanedResourcesFunction
    {
        [FunctionName("AWSOrphanedResourcesFunction")]
        //public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")] TimerInfo myTimer, ILogger log)
        //{
        public async Task RunAsync([TimerTrigger("%AwsCustomTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSOrphanedResources function executed at: {DateTime.Now}");

            bool IsBulkInsertResult = false;
            AmazonS3Client s3Client = new(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new();
            ListObjectsResponse MoveOldObjReqlist = new();
            string DestinationFolders = "orphanedresource/backup";

            List<string> accountIds = new();
            if (!string.IsNullOrEmpty(ConfigStore.Aws.AccountIds))
            {
                accountIds = ConfigStore.Aws.AccountIds.Split(",").ToList();
            }
            request.BucketName = ConfigStore.Aws.NewBucketName;
            request.Prefix = "orphanedresource";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("AccountID");
            sourceData.Columns.Add("ServiceCategory");
            sourceData.Columns.Add("ResourceID");
            sourceData.Columns.Add("Status");
            sourceData.Columns.Add("ResourceAssociated");
            sourceData.Columns.Add("Size");
            sourceData.Columns.Add("Type");
            sourceData.Columns.Add("Region");
            sourceData.Columns.Add("CreatedOn");
            sourceData.Columns.Add("Tags");

            try
            {
                //.OrderBy(a => a.LastModified).Take(5)
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
                                    row["ServiceCategory"] = dr["Service Category"];
                                    row["ResourceID"] = dr["ResourceID"];
                                    row["Status"] = dr["Status"];
                                    row["ResourceAssociated"] = dr["Resource_Associated"];
                                    row["Size"] = dr["Size"];
                                    row["Type"] = dr["Type"];
                                    row["Region"] = dr["Region"];
                                    row["CreatedOn"] = dr["CreatedOn"];
                                    row["Tags"] = dr["Tags"];
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
                        SqlCommand commandRowCount = new SqlCommand("Truncate table  " + "dbo.AWSOrphanedResources;", sourceConnection);
                        long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());
                        if (countStart == 0)
                        {
                            //Perform Bulk Insert Opertion to Source table
                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSOrphanedResources";
                            bcp.WriteToServer(sourceData);
                            IsBulkInsertResult = true;
                        }
                        sourceConnection.Close();
                    }
                }
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
                Prefix = "orphanedresource" // Only list objects in the specified source folder
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
