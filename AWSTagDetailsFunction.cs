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

namespace Budget.TimerFunction
{
    public class AWSTagDetailsFunction
    {
        [FunctionName("AWSTagDetailsFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSTagDetails function executed at: {DateTime.Now}");
            AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new ListObjectsRequest();
            ListObjectsResponse deleteObjReqlist = new();
            bool IsBulkInsertResult = false;

            request.BucketName = ConfigStore.Aws.BucketName;
            request.Prefix = "tagcomplaince";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("ServiceCategory");
            sourceData.Columns.Add("ServiceName");
            sourceData.Columns.Add("ResourceID");
            sourceData.Columns.Add("Tags");
            try
            {
                foreach (S3Object obj in res.S3Objects)
                {
                    if (obj.Size != 0)
                    {
                        if (obj.LastModified == DateTime.Today)
                        {
                            var response = s3Client.GetObjectAsync(ConfigStore.Aws.BucketName, obj.Key).Result;
                            using StreamReader reader = new StreamReader(response.ResponseStream);
                            using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                            using var dr = new CsvDataReader(csv);

                            while (dr.Read())
                            {
                                DataRow row = sourceData.NewRow();
                                row["ServiceCategory"] = dr["ServiceCategory"];
                                row["ServiceName"] = dr["ServiceName"];
                                row["ResourceID"] = dr["ResourceID"];
                                row["Tags"] = dr["Tags"];
                                sourceData.Rows.Add(row);
                            }
                        }
                        else
                        {
                            //Add old object to deleteObject List
                            deleteObjReqlist.S3Objects.Add(obj);
                        }
                        if (sourceData.Rows.Count > 0)
                        {
                            using (SqlConnection sourceConnection = new SqlConnection(ConfigStore.SQLConnectionString))
                            {
                                sourceConnection.Open();

                                // Check the count of existing data from the source table
                                SqlCommand commandRowCount = new SqlCommand("select count(*) FROM " + "dbo.AWSTagDetails;", sourceConnection);
                                long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());
                                if (countStart == 0)
                                {
                                    //Perform Bulk Insert Opertion to Source table
                                    SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                                    bcp.DestinationTableName = "AWSTagDetails";
                                    bcp.WriteToServer(sourceData);
                                    IsBulkInsertResult = true;
                                }
                                else
                                {
                                    // Perform an Delete operation for old data from the source table.
                                    commandRowCount = new SqlCommand("Truncate table  " + "dbo.AWSTagDetails;", sourceConnection);
                                    commandRowCount.ExecuteScalar();
                                }
                                sourceConnection.Close();
                            }
                        }
                        //After succesful bulk insert delete old files from s3 bucket
                        if (IsBulkInsertResult == true)
                        {
                            foreach (S3Object oldObj in deleteObjReqlist.S3Objects)
                            {
                                //Add Key name and Bucket name in deleteObjectRequest
                                DeleteObjectRequest deleteObjReq = new DeleteObjectRequest
                                {
                                    BucketName = oldObj.BucketName,
                                    Key = oldObj.Key
                                };
                                //Perform Delete operation 
                                await s3Client.DeleteObjectAsync(deleteObjReq);
                            }
                        }
                    }
                }
            }

            catch (Exception Excep)
            {
                Console.WriteLine(Excep.Message, Excep.InnerException);
            }
        }
    }
}
