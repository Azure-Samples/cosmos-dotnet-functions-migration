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
    using System.Runtime.Serialization.Json;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Xml.Linq;
    using System.Xml.XPath;

    public class DocumentFeedMigrator
    {
        private Container containerToStoreDocuments;
        private string pkname = "pk1";
        string targetPartitionKeyAttribute = Environment.GetEnvironmentVariable($"{"TargetPartitionKeyAttribute"}");
        static string sourcePartitionKeyMapping = Environment.GetEnvironmentVariable($"{"SourcePartitionKeyMapping"}");
        //string[] sourcePartitionKeyMaps = sourcePartitionKeyMapping.Split(',');

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
                foreach (Document doc in documents)
                {
                    dynamic json = JsonConvert.DeserializeObject(doc.ToString());
                    var jsonReader = JsonReaderWriterFactory.CreateJsonReader(Encoding.UTF8.GetBytes(doc.ToString()), new System.Xml.XmlDictionaryReaderQuotas());
                    var root = XElement.Load(jsonReader);
                    string newValue = root.XPathSelectElement(sourcePartitionKeyMapping).Value;
                    json[targetPartitionKeyAttribute] = newValue;
                    Document document = new Document();
                    JsonReader reader = new JTokenReader(json);                   
                    document.LoadFrom(reader);
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
    }
}