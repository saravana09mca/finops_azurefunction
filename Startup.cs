using System.IO;
using System;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;

[assembly: FunctionsStartup(typeof(Budget.TimerFunction.Startup))]

namespace Budget.TimerFunction
{
    public class Startup : FunctionsStartup
    {
        private static IConfigurationRoot config;

        public override void ConfigureAppConfiguration(IFunctionsConfigurationBuilder builder)
        {
            FunctionsHostBuilderContext context = builder.GetContext();
            config = new ConfigurationBuilder()
            .SetBasePath(Directory.GetCurrentDirectory())
            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
            .AddEnvironmentVariables()
            .Build();
            InitializeConfigStore();
        }

        public void InitializeConfigStore()
        {
            ConfigStore.AADAuthority = config.GetValue<string>("AADAuthority");
            ConfigStore.AADClientId = config.GetValue<string>("AADClientId");
            ConfigStore.AADTenantId = config.GetValue<string>("AADTenantId");
            ConfigStore.AADClientSecret = config.GetValue<string>("AADClientSecret");
            ConfigStore.DestinationStorageConn = config.GetValue<string>("DestinationStorageConn");
            ConfigStore.DestinationContainer = config.GetValue<string>("DestinationContainer");
            ConfigStore.DestinationBlobName = config.GetValue<string>("DestinationBlobName");
            ConfigStore.DestinationContainerHistory = config.GetValue<string>("DestinationContainerHistory");
            ConfigStore.DestinationBlobNameHistory = config.GetValue<string>("DestinationBlobNameHistory");
            ConfigStore.StartDate = config.GetValue<DateTime>("StartDate");

            ConfigStore.AzureStorageAccountConnectionString = config.GetValue<string>("AzureStorageAccountConnectionString");
            ConfigStore.GCP_BlobContrainerName = config.GetValue<string>("GCP_BlobContrainerName");
            ConfigStore.GCP_BlobFileName = config.GetValue<string>("GCP_BlobFileName");
            ConfigStore.GCP_DataSetId = config.GetValue<string>("GCP_DataSetId");
            ConfigStore.GCP_ProjectId = config.GetValue<string>("GCP_ProjectId");
            ConfigStore.GCP_TableId = config.GetValue<string>("GCP_TableId");
            ConfigStore.GCP_FromDate = config.GetValue<string>("GCP_FromDate");
            ConfigStore.GCP_ToDate = config.GetValue<string>("GCP_ToDate");
            ConfigStore.GCP_IsManualDateRange = config.GetValue<bool>("GCP_IsManualDateRange");
            ConfigStore.GCP_DataDaysDiff = config.GetValue<int>("GCP_DataDaysDiff");
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {   
        }
    }
}