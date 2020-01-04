//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

using System;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Fluent;
using Microsoft.Azure.Functions.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
[assembly: FunctionsStartup(typeof(MigrationExecutorFunctionApp.Startup))]

namespace MigrationExecutorFunctionApp
{

    public class Startup : FunctionsStartup
    {
        public override void Configure(IFunctionsHostBuilder builder)
        {
            string connectionString = Environment.GetEnvironmentVariable($"{"TargetCosmosDB"}");
            string database = Environment.GetEnvironmentVariable($"{"TargetDatabase"}");
            string collection = Environment.GetEnvironmentVariable($"{"TargetCollection"}");

            CosmosClient client = GetCustomClient(connectionString);
            builder.Services.AddSingleton<Container>(client.GetContainer(database, collection));
        }

        private static CosmosClient GetCustomClient(string connectionString)
        {
            CosmosClientBuilder builder = new CosmosClientBuilder(connectionString)
                .WithApplicationName("CosmosFunctionsMigration")
                .WithBulkExecution(true)
                .WithThrottlingRetryOptions(TimeSpan.FromSeconds(30), 10);

            return builder.Build();
        }
    }
}
