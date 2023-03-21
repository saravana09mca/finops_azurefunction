using System.IO;
using System;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Amazon.S3;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;

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
            ConfigStore.GCP_UtilizationDataDateDiff = config.GetValue<int>("GCP_UtilizationDataDateDiff");
            ConfigStore.GCP_UtilizationDatasetId = config.GetValue<string>("GCP_UtilizationDatasetId");
            ConfigStore.GCP_UtilizationProjectId = config.GetValue<string>("GCP_UtilizationProjectId");
            ConfigStore.GCP_UtilizationTableId = config.GetValue<string>("GCP_UtilizationTableId");
            ConfigStore.GCP_AdvisorProjectId = config.GetValue<string>("GCP_AdvisorProjectId");
            ConfigStore.GCP_AdvisorDatasetId = config.GetValue<string>("GCP_AdvisorDatasetId");
            ConfigStore.GCP_AdvisorTableId = config.GetValue<string>("GCP_AdvisorTableId");
            ConfigStore.GCP_AdvisorInsightsTableId = config.GetValue<string>("GCP_AdvisorInsightsTableId");

        }

        public override void Configure(IFunctionsHostBuilder builder)
        {
            builder.Services.AddSingleton<IAmazonS3>(AmazonS3ClientCreate);
        }

        private static IAmazonS3 AmazonS3ClientCreate(IServiceProvider serviceProvider)
        {
            var validationErrors = ValidateAwsConfig().ToList();
            if (validationErrors.Any())
                throw new InvalidOperationException(string.Join(Environment.NewLine, validationErrors));

            return new AmazonS3Client(ConfigStore.Aws.AccessKey, ConfigStore.Aws.SecretKey, ConfigStore.Aws.Region);
        }

        private static IEnumerable<string> ValidateAwsConfig()
        {
            if (string.IsNullOrEmpty(ConfigStore.Aws.AccessKey))
                yield return "Configuration setting 'AwsAccessKey' does not have a value set";

            if (string.IsNullOrEmpty(ConfigStore.Aws.SecretKey))
                yield return "Configuration setting 'AwsSecretKey' does not have a value set";

            if (string.IsNullOrEmpty(ConfigStore.Aws.BucketName))
                yield return "Configuration setting 'AwsBucketName' does not have a value set";
        }
    }
}