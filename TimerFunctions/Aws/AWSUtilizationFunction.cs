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

namespace Budget.TimerFunction.Aws
{
    public class AWSUtilizationFunction
    {
        [FunctionName("AWSUtilizationFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSUtilization function executed at: {DateTime.Now}");

            bool IsBulkInsertResult = false;
            AmazonS3Client s3Client = new(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new();
            ListObjectsResponse deleteObjReqlist = new();
            ListObjectsResponse LatestObjReqlist = new();

            request.BucketName = ConfigStore.Aws.BucketName;
            request.Prefix = "utilizationreports";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new();
            sourceData.Columns.Add("Id");
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

            try
            {
                foreach (S3Object obj in res.S3Objects)
                {
                    if (obj.Size != 0)
                    {
                        if (obj.LastModified.ToString("yyyy/MM/dd") == DateTime.Today.ToString("yyyy/MM/dd"))
                        {
                            //Add Latest object to Latest Object List
                            LatestObjReqlist.S3Objects.Add(obj);
                        }
                        else
                        {
                            //Add old object to deleteObject List
                            deleteObjReqlist.S3Objects.Add(obj);
                        }
                    }
                }
                //Select the latest Object based on LastModified
                S3Object LatestObject = LatestObjReqlist.S3Objects.OrderBy(a => a.LastModified).LastOrDefault();
                if (LatestObject != null)
                {
                    //Extract the Data from the CSV file
                    var response = s3Client.GetObjectAsync(ConfigStore.Aws.BucketName, LatestObject.Key).Result;
                    using StreamReader reader = new StreamReader(response.ResponseStream);
                    using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
                    using var dr = new CsvDataReader(csv);

                    while (dr.Read())
                    {
                        DataRow row = sourceData.NewRow();
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
                        sourceData.Rows.Add(row);
                    }
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
                //After succesful bulk insert delete old files from s3 bucket
                if (IsBulkInsertResult == true && deleteObjReqlist.S3Objects.Count > 0)
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
