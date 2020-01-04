//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorFunctionApp
{
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.WebJobs;
    using Microsoft.Extensions.Logging;
    
    public class PostMortemDocumentsProcessor
    {
        private Container containerToStoreDocuments;

        public PostMortemDocumentsProcessor(Container containerToStoreDocuments)
        {
            this.containerToStoreDocuments = containerToStoreDocuments;
        }
         
        [FunctionName("PostMortemDocumentsProcessor")]
        public async Task Run(
            [QueueTrigger("%QueueName%", Connection = "QueueConnectionString")]Document myQueueItem, 
            ILogger log,
            CancellationToken cancellationToken)
        {
            try
            {
                await containerToStoreDocuments.CreateItemAsync(item: myQueueItem, cancellationToken: cancellationToken);
            }
            catch (CosmosException e)
            {
                log.LogError(e, e.Message);
            }
        }
    }
}
