﻿using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace Budget.TimerFunction
{
    public class Helper
    {
        public static Stream GetBlobMemoryStream(string storageAccountConnectionString,string containerName,string blobName)
        {

            var memoryStream = new MemoryStream();         
            var storageAccount = CloudStorageAccount.Parse(storageAccountConnectionString);
            var blobClient = storageAccount.CreateCloudBlobClient();
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            blob.DownloadToStream(memoryStream);
            memoryStream.Position = 0;
            return memoryStream;
        }
        public static decimal ValidateDecimal(string input)
        {
            decimal number;
            if (!Decimal.TryParse(input, out number))
            {
                number = Decimal.Parse(input, NumberStyles.AllowExponent | NumberStyles.AllowDecimalPoint);
            }
            return number;
        }
        public static string GetSeverity(string input)
        { 
        string severity=string.Empty;
            switch (input.ToLower()) {
                case "p1":
                    severity = "Low";
                    break;
                case "p2":
                    severity = "Medium";
                    break;
                case "p3":
                case "p4":
                    severity = "High";
                    break;
                default:
                    severity = input;
                    break;
                    
            }
            return severity;
        }
        public static string GetConfigValueByKey(string Key) {
            string value = string.Empty;
            // Set up the SQL connection and command
            using (SqlConnection connection = new SqlConnection(Environment.GetEnvironmentVariable("sqlconnectionstring")))
            {
                connection.Open();
                using (SqlCommand command = new SqlCommand("SELECT ConfigValue FROM Configuration where ConfigKey='"+ Key + "' and IsActive=1", connection))
                {
                    // Execute the command and retrieve the scalar value
                     value = (string)command.ExecuteScalar();
                   
                }
            }
            return value;
        }
    }
}
