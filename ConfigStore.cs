using System;

namespace Budget.TimerFunction
{
    public class ConfigStore
    {
        public static string AADAuthority { get; set; }
        public static string AADClientId { get; set; }
        public static string AADTenantId { get; set; }
        public static string AADClientSecret { get; set; }
        public static string DestinationStorageConn { get; set; }
        public static string DestinationContainer { get; set; }
        public static string DestinationBlobName { get; set; }
        public static string DestinationContainerHistory { get; set; }
        public static string DestinationBlobNameHistory { get; set; }
        public static DateTime StartDate { get; set; }

        public static string AzureStorageAccountConnectionString { get; set; }
        public static string GCP_BlobContrainerName { get; set; }
        public static string GCP_BlobFileName { get; set; }
        public static string GCP_DataSetId { get; set; }
        public static string GCP_ProjectId { get; set; }
        public static string GCP_TableId { get; set; }
        public static string GCP_FromDate { get; set; }
        public static string GCP_ToDate { get; set; }
        public static int GCP_DataDaysDiff { get; set; }
        public static bool GCP_IsManualDateRange { get; set; }
    }
}