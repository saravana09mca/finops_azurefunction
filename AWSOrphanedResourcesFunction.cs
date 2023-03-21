using System;
using Amazon.Runtime;
using Amazon.S3.Model;
using Amazon.S3;
using CsvHelper;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;
using System.IO;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace Budget.TimerFunction
{
    public class AWSOrphanedResourcesFunction
    {
        [FunctionName("AWSOrphanedResourcesFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSOrphanedResources function executed at: {DateTime.Now}");

            bool IsBulkInsertResult = false;
            AmazonS3Client s3Client = new(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new();
            ListObjectsResponse deleteObjReqlist = new();

            request.BucketName = ConfigStore.Aws.BucketName;
            request.Prefix = "orphanedresource";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("ServiceCategory");          
            sourceData.Columns.Add("ResourceID");
            sourceData.Columns.Add("Status");
            sourceData.Columns.Add("InstanceAssociated");
            sourceData.Columns.Add("Size");
            sourceData.Columns.Add("Type");
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
                                row["ServiceCategory"] = dr["Service Category"];
                                row["ResourceID"] = dr["ResourceID"];
                                row["Status"] = dr["Status"];
                                row["InstanceAssociated"] = dr["Instance_Associated"];
                                row["Size"] = dr["Size"];
                                row["Type"] = dr["Type"];
                                row["Tags"] = dr["Tags"];
                                sourceData.Rows.Add(row);
                            }
                        }
                        else
                        {
                            //Add old object to deleteObject List
                            deleteObjReqlist.S3Objects.Add(obj);
                        }
                    }
                }
                if (sourceData.Rows.Count > 0)
                {
                    using (SqlConnection sourceConnection = new SqlConnection(ConfigStore.SQLConnectionString))
                    {
                        sourceConnection.Open();
                        // Check the count of existing data from the source table
                        SqlCommand commandRowCount = new SqlCommand("select count(*) FROM " + "dbo.AWSOrphanedResources;", sourceConnection);
                        long countStart = System.Convert.ToInt32(commandRowCount.ExecuteScalar());

                        if (countStart == 0)
                        {
                            //Perform Bulk Insert Opertion to Source table
                            SqlBulkCopy bcp = new SqlBulkCopy(ConfigStore.SQLConnectionString);
                            bcp.DestinationTableName = "AWSOrphanedResources";                            
                            bcp.WriteToServer(sourceData);
                            IsBulkInsertResult = true;
                        }
                        else
                        {
                            // Perform an Delete operation for old data from the source table.
                            commandRowCount = new SqlCommand("Truncate table  " + "dbo.AWSOrphanedResources;", sourceConnection);
                            commandRowCount.ExecuteScalar();
                        }
                        sourceConnection.Close();
                    }
                }
                //After succesful bulk insert delete old files from s3 bucket
                if(IsBulkInsertResult==true)
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

            catch (Exception Excep)
            {
                Console.WriteLine(Excep.Message, Excep.InnerException);
            }
        }
    }
}
