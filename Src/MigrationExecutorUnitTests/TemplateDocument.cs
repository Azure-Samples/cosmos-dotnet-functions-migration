//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationExecutorUnitTests
{
    using Newtonsoft.Json;

    internal sealed class TemplateDocument
    {
        public TemplateDocument(string id, string pk, bool addDefaultproperties)
        {
            this.Id = id;
            this.Pk = pk;

            if (addDefaultproperties)
            {
                this.Price = 49.99;
                this.Item = "Adidas Compression Shorts";
                
            }
        }

        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }

        [JsonProperty(PropertyName = "pk")]
        public string Pk { get; set; }

        [JsonProperty(PropertyName = "price")]
        public double Price { get; set; }

        [JsonProperty(PropertyName = "item")]
        public string Item { get; set; }
    }
}


