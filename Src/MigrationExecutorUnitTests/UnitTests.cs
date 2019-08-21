//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace UnitTestProject1
{ 
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Extensions.Configuration;
    using Microsoft.Extensions.Logging;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using MigrationExecutorFunctionApp;
    using MigrationExecutorUnitTests;
    using Moq;

    [TestClass]
    public class UnitTests
    {
        private static IConfigurationRoot configuration = new ConfigurationBuilder()
                .SetBasePath(Environment.CurrentDirectory)
                .AddJsonFile("AppSettings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

        [TestMethod]
        public async Task TestIfDocumentsAreUpserted()
        {
            Mock<IBulkExecutor> mockBulkExecutor = new Mock<IBulkExecutor>();
            Mock<ILogger> mockLog = new Mock<ILogger>();
          
            AsyncCollector<Document> postMortemCol = new AsyncCollector<Document>();

            DocumentClient client = new DocumentClient(new Uri(configuration["EndPoint"]), configuration["AuthKey"]);

            DocumentCollection container = client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(configuration["TargetDatabase"]))
                .Where(c => c.Id == configuration["TargetCollection"]).AsEnumerable().FirstOrDefault();

            IBulkExecutor bulkExecutor = new BulkExecutor(client, container);
            await bulkExecutor.InitializeAsync();

            IEnumerable<string> bulkDocs = Utilities.GenerateDocumentsWithRandomIdAndPk(5000);
            BulkImportResponse bulkImportResponse = await bulkExecutor.BulkImportAsync(bulkDocs, false);

            List<Document> fakeBadDocsBatch = new List<Document>();
            Document doc = new Document();
            doc.Id = "0f4adabc-d461-495f-bdd3-4f8877ae7f3f";

            for (int i = 0; i < 10; i++)
            {
                fakeBadDocsBatch.Add(doc);
            }

            ReadOnlyCollection<Document> readOnlyDocs = fakeBadDocsBatch.AsReadOnly();

            mockBulkExecutor.Setup(bulkExecutorFake => bulkExecutorFake.InitializeAsync())
                .Verifiable();

            mockBulkExecutor.Setup(bulkExecutorFake => bulkExecutorFake.BulkImportAsync(It.IsAny<ReadOnlyCollection<Document>>(), true, true, null, It.IsAny<int>(), It.IsAny<CancellationToken>()))
                .Returns(() => Task.FromResult(bulkImportResponse))

                //Add docs to the badInputDocuments list to test whether the post-mortem queue is employed
                .Callback(() => bulkImportResponse.BadInputDocuments.AddRange(fakeBadDocsBatch));

            DocumentFeedMigrator migrator = new DocumentFeedMigrator(mockBulkExecutor.Object);
            await migrator.Run(postMortemCol, readOnlyDocs, mockLog.Object);
          
            Assert.AreEqual(postMortemCol.Count(), 10);

            mockBulkExecutor.Verify(
                bulkExecutorFake => 
            bulkExecutorFake.BulkImportAsync(
                It.IsAny<ReadOnlyCollection<Document>>(),
                true,
                true,
                null,
                It.IsAny<int>(),
                It.IsAny<CancellationToken>()),
                Times.Exactly(1));
        }

        [TestMethod]
        public async Task TestIfQueueTriggerUpsertsDocs()
        {
            Mock<IDocumentClient> mockClient = new Mock<IDocumentClient>();
            Mock<ILogger> fakeLog = new Mock<ILogger>();

            Document doc = new Document();
            doc.Id = "68eaf565-b754-4cdb-a40c-433c7fd0f39e";

            int callTimesUpsert = 0;

            Uri collectionLink = UriFactory.CreateDocumentCollectionUri("alpha", "target");
            mockClient.Setup(client => client.UpsertDocumentAsync(It.IsAny<Uri>(), It.IsAny<Document>(), null, false, default))
                .Returns(() => Task.FromResult(new ResourceResponse<Document>()))
                .Callback(() => callTimesUpsert++);

            PostMortemDocumentsProcessor postMortemFunction = new PostMortemDocumentsProcessor(collectionLink);
            await postMortemFunction.Run(mockClient.Object, doc, fakeLog.Object);

            Assert.AreEqual(callTimesUpsert, 1);
        }
    }
}
