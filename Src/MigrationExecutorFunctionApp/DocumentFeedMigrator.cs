//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorFunctionApp
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;

    public class DocumentFeedMigrator
    {
        private IBulkExecutor bulkExecutor;

        public DocumentFeedMigrator(IBulkExecutor bulkExecutor)
        {
            this.bulkExecutor = bulkExecutor;
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
            ILogger log)
        {
            if (documents != null && documents.Count > 0)
            {
                BulkImportResponse bulkImportResponse = null;

                CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
                CancellationToken cancellationToken = cancellationTokenSource.Token;

                List<Task> tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        bulkImportResponse = await bulkExecutor.BulkImportAsync(
                        documents: documents,
                        enableUpsert: true,
                        disableAutomaticIdGeneration: true,
                        maxInMemorySortingBatchSize: 10000000,
                        cancellationToken: cancellationToken);
                    }
                    catch (DocumentClientException e)
                    {
                        log.LogError("Document client Exception: {0}", e);
                    }
                    catch (Exception e)
                    {
                        log.LogError("Exception: {0}", e);
                    }

                    if (bulkImportResponse.BadInputDocuments != null && bulkImportResponse.BadInputDocuments.Count > 0)
                    {
                        foreach (Document doc in bulkImportResponse.BadInputDocuments)
                        {
                            await postMortemQueue.AddAsync(doc);
                            log.LogInformation("Document added to the post-mortem queue: {0}", doc.Id);
                        }
                    }
                },
                cancellationToken));

                await Task.WhenAll(tasks);

                log.LogMetric("The Number of Documents Imported", bulkImportResponse.NumberOfDocumentsImported);
                log.LogMetric("The Total Number of RU/s consumed", bulkImportResponse.TotalRequestUnitsConsumed);
                log.LogMetric("RU/s per Document Write", bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported);
                log.LogMetric("RU/s being used", bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds);
                log.LogMetric("Migration Time", bulkImportResponse.TotalTimeTaken.TotalMinutes);
            }
        }
    }
}