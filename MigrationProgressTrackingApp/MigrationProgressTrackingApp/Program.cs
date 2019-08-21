//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationProgressTrackingApp
{
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Extensions.Configuration;
    using System;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;

    public class Program
    {
        const int SleepTime = 10000;

        private static DateTime start = DateTime.Now;
        private static IConfigurationRoot config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("AppSettings.json")
                .AddEnvironmentVariables()
                .Build();

        private long sourceCollectionCount = 0;
        private double currentPercentage = 0;
        private long prevDestinationCollectionCount = 0;
        private long currentDestinationCollectionCount = 0;
        private double totalInserted = 0;

        public static void Main(string[] args)
        {
            Program program = new Program();
            program.RunAsync().Wait();
        }

        public async Task RunAsync()
        {
            MigrationConfig configInstance = new MigrationConfig(
                config["SourceEndPoint"],
                config["SourceAuthKey"],
                config["SourceDatabase"],
                config["SourceCollection"],
                config["TargetEndPoint"],
                config["TargetAuthKey"],
                config["TargetDatabase"],
                config["TargetCollection"]);

            while (true)
            {
                await this.TrackMigrationProgressAsync(configInstance);
                await Task.Delay(SleepTime);
            }
        }

        private async Task TrackMigrationProgressAsync(MigrationConfig migrationConfig)
        {
            using (DocumentClient sourceClient = new DocumentClient(
                new Uri(migrationConfig.MonitoredUri),
                migrationConfig.MonitoredSecretKey))
            {
                sourceClient.ConnectionPolicy.RetryOptions = new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 1000, MaxRetryWaitTimeInSeconds = 1000 };
                using (DocumentClient destinationClient = new DocumentClient(
                    new Uri(migrationConfig.DestUri),
                    migrationConfig.DestSecretKey))
                {
                    destinationClient.ConnectionPolicy.RetryOptions = new RetryOptions { MaxRetryAttemptsOnThrottledRequests = 1000, MaxRetryWaitTimeInSeconds = 1000 };

                    RequestOptions options = new RequestOptions()
                    {
                        PopulateQuotaInfo = true,
                        PopulatePartitionKeyRangeStatistics = true
                    };

                    ResourceResponse<DocumentCollection> sourceCollection = await sourceClient.ReadDocumentCollectionAsync(
                                    UriFactory.CreateDocumentCollectionUri(migrationConfig.MonitoredDbName, migrationConfig.MonitoredCollectionName), options);

                    this.sourceCollectionCount = sourceCollection.Resource.PartitionKeyRangeStatistics
                        .Sum(pkr => pkr.DocumentCount);

                    ResourceResponse<DocumentCollection> destinationCollection = await destinationClient.ReadDocumentCollectionAsync(
                        UriFactory.CreateDocumentCollectionUri(migrationConfig.DestDbName, migrationConfig.DestCollectionName), options);

                    this.currentDestinationCollectionCount = destinationCollection.Resource.PartitionKeyRangeStatistics
                        .Sum(pkr => pkr.DocumentCount);

                    this.currentPercentage = this.sourceCollectionCount == 0 ? 100 : this.currentDestinationCollectionCount * 100.0 / this.sourceCollectionCount;

                    double currentRate = (this.currentDestinationCollectionCount - this.prevDestinationCollectionCount) * 1000.0 / SleepTime;
                    this.totalInserted += this.prevDestinationCollectionCount == 0 ? 0 : this.currentDestinationCollectionCount - this.prevDestinationCollectionCount;

                    long totalSeconds = (long)((DateTime.Now - start).TotalMilliseconds) / 1000;
                    double averageRate = this.totalInserted * 1.0 / totalSeconds;
                    double eta = averageRate == 0 ? 0 : (this.sourceCollectionCount - this.currentDestinationCollectionCount) * 1.0 / (averageRate * 3600);

                    this.TrackMetrics(this.sourceCollectionCount, this.currentDestinationCollectionCount, currentRate, averageRate, eta);

                    this.prevDestinationCollectionCount = this.currentDestinationCollectionCount;

                }
            }
        }

        private void TrackMetrics(long sourceCollectionCount, long currentDestinationCollectionCount, double currentRate, double averageRate, double eta)
        {
            Console.WriteLine("CurrentPercentage = " + currentPercentage, currentPercentage);
            Console.WriteLine("ETA = " + eta);
            Console.WriteLine("Current rate = " + currentRate);
            Console.WriteLine("Average rate = " + averageRate);
            Console.WriteLine("Source count = " + sourceCollectionCount);
            Console.WriteLine("Destination count = " + currentDestinationCollectionCount);
            Console.WriteLine("***********************");
        }
    }
}