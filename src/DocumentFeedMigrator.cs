//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorFunctionApp
{
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;



    public class DocumentFeedMigrator
    {
        private Container containerToStoreDocuments;
        string targetPartitionKeyAttribute = Environment.GetEnvironmentVariable($"{"TargetPartitionKeyAttribute"}");
        static string sourcePartitionKeyMapping = Environment.GetEnvironmentVariable($"{"SourcePartitionKeyMapping"}");    
        Boolean isSyntheticKey = Environment.GetEnvironmentVariable($"{"SourcePartitionKeyMapping"}").IndexOf(",") == -1? false: true;
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
                    document = sourcePartitionKeyMapping != null ? MapPartitionKey(doc): document = doc;
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

        public Document MapPartitionKey(Document doc)
        {
            if(isSyntheticKey)
            {
                doc = CreateSyntheticKey(doc);
            }
            else
            {
                doc.SetPropertyValue(targetPartitionKeyAttribute, doc.GetPropertyValue<string>(sourcePartitionKeyMapping));
            }
            return doc;
        }

        public Document CreateSyntheticKey(Document doc)
        {
            StringBuilder syntheticKey = new StringBuilder();
            string[] sourceAttributeArray = sourcePartitionKeyMapping.Split(',');
            int arraylength = sourceAttributeArray.Length;
            int count = 1;
            foreach (string attribute in sourceAttributeArray)
            {
                if (count == arraylength)
                {
                    syntheticKey.Append(doc.GetPropertyValue<string>(attribute));
                }
                else
                {
                    syntheticKey.Append(doc.GetPropertyValue<string>(attribute) + "-");
                }
                count++;
            }
            doc.SetPropertyValue(targetPartitionKeyAttribute, syntheticKey.ToString());
            return doc;
        }
    }
}