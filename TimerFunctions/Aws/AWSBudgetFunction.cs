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
    public class AWSBudgetFunction
    {
        [FunctionName("AWSBudgetFunction")]
        public async Task RunAsync([TimerTrigger("%AwsWeekelyTimer%")] TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"AWSBudget function executed at: {DateTime.Now}");
            AmazonS3Client s3Client = new AmazonS3Client(new BasicAWSCredentials(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey), Amazon.RegionEndpoint.USEast1);
            ListObjectsRequest request = new ListObjectsRequest();
            ListObjectsResponse deleteObjReqlist = new();
            ListObjectsResponse LatestObjReqlist = new();
            bool IsBulkInsertResult = false;

            request.BucketName = ConfigStore.Aws.BucketName;
            request.Prefix = "budgetreports";
            ListObjectsResponse res = await s3Client.ListObjectsAsync(request);
            DataTable sourceData = new DataTable();
            sourceData.Columns.Add("Id");
            sourceData.Columns.Add("BudgetName");
            sourceData.Columns.Add("BudgetCost");
            sourceData.Columns.Add("CurrentCost");
            sourceData.Columns.Add("ForecastedCost");
            sourceData.Columns.Add("FiltersApplied");
            sourceData.Columns.Add("BudgetPeriod");
            sourceData.Columns.Add("Status");
            sourceData.Columns.Add("StartDate");
            sourceData.Columns.Add("EndDate");
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
                        row["BudgetName"] = dr["BudgetName"];
                        row["BudgetCost"] = dr["BudgetCost"];
                        row["CurrentCost"] = dr["CurrentCost"];
                        row["ForecastedCost"] = dr["Forecasted Cost"];
                        row["FiltersApplied"] = dr["Filters Applied"];
                        row["BudgetPeriod"] = dr["Budget Period"];
                        row["Status"] = dr["Status"];
                        row["StartDate"] = dr["Start Date"];
                        row["EndDate"] = dr["End Date"];
                        sourceData.Rows.Add(row);
                    }
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
                            bcp.DestinationTableName = "AWSBudgetData";
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
