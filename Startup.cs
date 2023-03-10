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
        }

        public override void Configure(IFunctionsHostBuilder builder)
        {   
        }
    }
}