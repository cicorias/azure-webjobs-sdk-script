// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.ObjectModel;
using System.IO;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Script.Extensibility;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Script
{
    /// <summary>
    /// Enables all Core SDK Triggers/Binders
    /// </summary>
    [CLSCompliant(false)]
    public class WebJobsCoreScriptBindingProvider : ScriptBindingProvider
    {
        public WebJobsCoreScriptBindingProvider(JobHostConfiguration config, JObject hostMetadata, TraceWriter traceWriter) 
            : base(config, hostMetadata, traceWriter)
        {
        }

        public override void Initialize()
        {
            // Apply Queues configuration
            JObject configSection = (JObject)Metadata["queues"];
            JToken value = null;
            if (configSection != null)
            {
                if (configSection.TryGetValue("maxPollingInterval", out value))
                {
                    Config.Queues.MaxPollingInterval = TimeSpan.FromMilliseconds((int)value);
                }
                if (configSection.TryGetValue("batchSize", out value))
                {
                    Config.Queues.BatchSize = (int)value;
                }
                if (configSection.TryGetValue("maxDequeueCount", out value))
                {
                    Config.Queues.MaxDequeueCount = (int)value;
                }
                if (configSection.TryGetValue("newBatchThreshold", out value))
                {
                    Config.Queues.NewBatchThreshold = (int)value;
                }
            }
        }

        public override bool TryCreate(JObject bindingMetadata, out ScriptBinding binding)
        {
            binding = null;

            string type = (string)bindingMetadata.GetValue("type", StringComparison.OrdinalIgnoreCase);
            if (string.Compare(type, "queueTrigger", StringComparison.OrdinalIgnoreCase) == 0 ||
                string.Compare(type, "queue", StringComparison.OrdinalIgnoreCase) == 0)
            {
                binding = new QueueScriptBinding(bindingMetadata);
            }
            else if (string.Compare(type, "blobTrigger", StringComparison.OrdinalIgnoreCase) == 0 ||
                     string.Compare(type, "blob", StringComparison.OrdinalIgnoreCase) == 0)
            {
                binding = new BlobScriptBinding(bindingMetadata);
            }

            return binding != null;
        }

        private class QueueScriptBinding : ScriptBinding
        {
            public QueueScriptBinding(JObject metadata) : base(metadata)
            {
            }

            public override Collection<Attribute> GetAttributes()
            {
                Collection<Attribute> attributes = new Collection<Attribute>();

                string queueName = GetValue<string>("queueName");
                if (IsTrigger)
                {
                    attributes.Add(new QueueTriggerAttribute(queueName));
                }
                else
                {
                    attributes.Add(new QueueAttribute(queueName));
                }

                string account = GetValue<string>("connection");
                if (!string.IsNullOrEmpty(account))
                {
                    attributes.Add(new StorageAccountAttribute(account));
                }

                return attributes;
            }

            override public Type DefaultType
            {
                get
                {
                    if (Access == FileAccess.Read)
                    {
                        string dataType = GetValue<string>("dataType");
                        return string.Compare("binary", dataType, StringComparison.OrdinalIgnoreCase) == 0
                            ? typeof(byte[]) : typeof(string);
                    }
                    else
                    {
                        return typeof(IAsyncCollector<byte[]>);
                    }
                }
            }
        }

        private class BlobScriptBinding : ScriptBinding
        {
            public BlobScriptBinding(JObject metadata) : base(metadata)
            {
            }

            public override Collection<Attribute> GetAttributes()
            {
                Collection<Attribute> attributes = new Collection<Attribute>();

                string path = GetValue<string>("path");
                if (IsTrigger)
                {
                    attributes.Add(new BlobTriggerAttribute(path));
                }
                else
                {
                    attributes.Add(new BlobAttribute(path, Access));
                }

                string account = GetValue<string>("connection");
                if (!string.IsNullOrEmpty(account))
                {
                    attributes.Add(new StorageAccountAttribute(account));
                }

                return attributes;
            }

            override public Type DefaultType
            {
                get
                {
                    return typeof(Stream);
                }
            }
        }
    }
}
