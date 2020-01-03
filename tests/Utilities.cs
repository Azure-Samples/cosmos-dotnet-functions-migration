//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorUnitTests
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json;

    public static class Utilities
    {
        public static IEnumerable<string> GenerateDocumentsWithRandomIdAndPk(int count)
        {
            for (int i = 0; i < count; i++)
            {
                yield return JsonConvert.SerializeObject(new TemplateDocument(Guid.NewGuid().ToString(), Guid.NewGuid().ToString(), true));
            }
        }
    }
}
