using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using System;
using System.Collections.Generic;
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
    }
}
