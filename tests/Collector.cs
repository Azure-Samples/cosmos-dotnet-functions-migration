//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorUnitTests
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Azure.WebJobs;
    using System.Threading;
    using System.Threading.Tasks;

    public class AsyncCollector<T> : IAsyncCollector<T>
    {
        public readonly List<T> Items = new List<T>();

        public Task AddAsync(T item, CancellationToken cancellationToken = default(CancellationToken))
        {
            Items.Add(item);

            return Task.FromResult(true);
        }

        public Task FlushAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Task.FromResult(true);
        }
        public int Count()
        {
            return this.Items.Count;
        }

        public T GetElement(int i)
        {
            if (this.Items.Count > i && i >= 0)
            {
                return this.Items[i];
            }
            else
            {
                throw new ArgumentException("Index out of range");
            }
        }
    }
}
