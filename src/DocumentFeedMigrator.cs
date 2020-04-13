//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorFunctionApp
{
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class DocumentFeedMigrator
    {
        private Container containerToStoreDocuments;
        string targetPartitionKeyAttribute = Environment.GetEnvironmentVariable($"{"TargetPartitionKeyAttribute"}");
        static string sourcePartitionKeyMapping = Environment.GetEnvironmentVariable($"{"SourcePartitionKeyMapping"}");
        public DocumentFeedMigrator(Container containerToStoreDocuments)
        {
            this.containerToStoreDocuments = containerToStoreDocuments;
        }

        [FunctionName("DocumentFeedMigrator")]
        public async Task Run(
            [Queue("%QueueName%", Connection = "QueueConnectionString")]IAsyncCollector<Document> postMortemQueue,
            [CosmosDBTrigger(
            databaseName: "%SourceDatabase%",
            collectionName: "%SourceCollection%",
            ConnectionStringSetting = "SourceCosmosDB",
            LeaseCollectionName = "leases",
            StartFromBeginning = true,
            MaxItemsPerInvocation = 10000000,
            CreateLeaseCollectionIfNotExists = true)
            ]IReadOnlyList<Document> documents,
            ILogger log,
            CancellationToken cancellationToken)
        {
            if (documents != null && documents.Count > 0)
            {
                ConcurrentDictionary<int, int> failedMetrics = new ConcurrentDictionary<int, int>();
                List<Task> tasks = new List<Task>();
                Stopwatch stopwatch = Stopwatch.StartNew();
                Document document = new Document();
                foreach (Document doc in documents)
                {
                    document = sourcePartitionKeyMapping != null ? MapPartitionKey(document, doc): document = doc;
                    tasks.Add(this.containerToStoreDocuments.CreateItemAsync(item: document, cancellationToken: cancellationToken).ContinueWith((Task<ItemResponse<Document>> task) =>
                    {
                        AggregateException innerExceptions = task.Exception.Flatten();
                        CosmosException cosmosException = (CosmosException)innerExceptions.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException);
                        failedMetrics.AddOrUpdate((int)cosmosException.StatusCode, 0, (key, value) => value + 1);
                        return postMortemQueue.AddAsync(document);
                    }, TaskContinuationOptions.OnlyOnFaulted));
                }
                await Task.WhenAll(tasks);
                stopwatch.Stop();

                int totalFailedItems = 0;
                foreach (KeyValuePair<int, int> failedMetric in failedMetrics)
                {
                    log.LogMetric($"Failed items with StatusCode {failedMetric.Key}", failedMetric.Value);
                    totalFailedItems += failedMetric.Value;
                }
                log.LogMetric("Documents migrated", documents.Count - totalFailedItems);
                log.LogMetric("Migration Time in ms", stopwatch.ElapsedMilliseconds);
            }

        }

        public Document MapPartitionKey(Document document, Document doc)
        {
            dynamic json = JsonConvert.DeserializeObject(doc.ToString());
            StringBuilder syntheticKey = new StringBuilder();
            string[] sourceAttributeArray = sourcePartitionKeyMapping.Split(',');
            foreach (string attribute in sourceAttributeArray) {
                syntheticKey.Append(json[attribute]);
            }
            json[targetPartitionKeyAttribute] = syntheticKey.ToString();
            JsonReader reader = new JTokenReader(json);
            document.LoadFrom(reader);
            return document;
        }
    }
}