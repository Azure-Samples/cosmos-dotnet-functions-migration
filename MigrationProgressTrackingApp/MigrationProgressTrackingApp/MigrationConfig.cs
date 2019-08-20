//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace MigrationProgressTrackingApp
{
    using Newtonsoft.Json;

    public class MigrationConfig
    {
        public MigrationConfig(
            string monitoredUri,
            string monitoredSecretKey,
            string monitoredDbName,
            string monitoredCollectionName,
            string destUri,
            string destKey,
            string destDbName,
            string destCollectionName)
        {
            this.MonitoredUri = monitoredUri;
            this.MonitoredSecretKey = monitoredSecretKey;
            this.MonitoredDbName = monitoredDbName;
            this.MonitoredCollectionName = monitoredCollectionName;
            this.DestUri = destUri;
            this.DestSecretKey = destKey;
            this.DestDbName = destDbName;
            this.DestCollectionName = destCollectionName;
        }

        [JsonProperty("monitoredUri")]
        public string MonitoredUri { get; set; }

        [JsonProperty("monitoredSecretKey")]
        public string MonitoredSecretKey { get; set; }

        [JsonProperty("monitoredDbName")]
        public string MonitoredDbName { get; set; }

        [JsonProperty("monitoredCollectionName")]
        public string MonitoredCollectionName { get; set; }
        [JsonProperty("destUri")]
        public string DestUri { get; set; }

        [JsonProperty("destSecretKey")]
        public string DestSecretKey { get; set; }

        [JsonProperty("destDbName")]
        public string DestDbName { get; set; }

        [JsonProperty("destCollectionName")]
        public string DestCollectionName { get; set; }
    }
}
