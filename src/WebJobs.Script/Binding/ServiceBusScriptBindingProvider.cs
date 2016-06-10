// Copyright (c) .NET Foundation. All rights reserved.
// Licensed under the MIT License. See License.txt in the project root for license information.

using System;
using System.Collections.ObjectModel;
using System.IO;
using System.Reflection;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.WebJobs.Script.Extensibility;
using Microsoft.Azure.WebJobs.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json.Linq;

namespace Microsoft.Azure.WebJobs.Script
{
    [CLSCompliant(false)]
    public class ServiceBusScriptBindingProvider : ScriptBindingProvider
    {
        private readonly EventHubConfiguration _eventHubConfiguration;

        public ServiceBusScriptBindingProvider(JobHostConfiguration config, JObject hostMetadata, TraceWriter traceWriter)
            : base(config, hostMetadata, traceWriter)
        {
            _eventHubConfiguration = new EventHubConfiguration();
        }

        public override void Initialize()
        {
            // Apply ServiceBus configuration
            ServiceBusConfiguration serviceBusConfig = new ServiceBusConfiguration();
            JObject configSection = (JObject)Metadata.GetValue("serviceBus", StringComparison.OrdinalIgnoreCase);
            JToken value = null;
            if (configSection != null)
            {
                if (configSection.TryGetValue("maxConcurrentCalls", StringComparison.OrdinalIgnoreCase, out value))
                {
                    serviceBusConfig.MessageOptions.MaxConcurrentCalls = (int)value;
                }
            }

            Config.UseServiceBus(serviceBusConfig);
            Config.UseEventHub(_eventHubConfiguration);
        }

        public override bool TryCreate(JObject bindingMetadata, out ScriptBinding binding)
        {
            binding = null;

            string type = (string)bindingMetadata.GetValue("type", StringComparison.OrdinalIgnoreCase);
            if (string.Compare(type, "serviceBusTrigger", StringComparison.OrdinalIgnoreCase) == 0 ||
                string.Compare(type, "serviceBus", StringComparison.OrdinalIgnoreCase) == 0)
            {
                binding = new ServiceBusScriptBinding(bindingMetadata);
            }
            if (string.Compare(type, "eventHubTrigger", StringComparison.OrdinalIgnoreCase) == 0 ||
                string.Compare(type, "eventHub", StringComparison.OrdinalIgnoreCase) == 0)
            {
                binding = new EventHubScriptBinding(Config, _eventHubConfiguration, bindingMetadata);
            }

            return binding != null;
        }

        public override bool TryResolveAssembly(string assemblyName, out Assembly assembly)
        {
            assembly = null;

            if (string.Compare(assemblyName, "Microsoft.ServiceBus", StringComparison.OrdinalIgnoreCase) == 0)
            {
                assembly = typeof(BrokeredMessage).Assembly;
            }

            return assembly != null;
        }

        private class EventHubScriptBinding : ScriptBinding
        {
            private readonly string _storageConnectionString;
            private readonly EventHubConfiguration _eventHubConfiguration;

            public EventHubScriptBinding(JobHostConfiguration hostConfig, EventHubConfiguration eventHubConfig, JObject metadata) : base(metadata)
            {
                _eventHubConfiguration = eventHubConfig;
                _storageConnectionString = hostConfig.StorageConnectionString;
            }

            public override Collection<Attribute> GetAttributes()
            {
                Collection<Attribute> attributes = new Collection<Attribute>();

                string eventHubName = GetValue<string>("path");
                string connectionString = GetValue<string>("connection");
                if (!string.IsNullOrEmpty(connectionString))
                {
                    connectionString = Utility.GetAppSettingOrEnvironmentValue(connectionString);
                }

                if (IsTrigger)
                {
                    attributes.Add(new EventHubTriggerAttribute(eventHubName));

                    string eventProcessorHostName = Guid.NewGuid().ToString();
                    string storageConnectionString = _storageConnectionString;

                    string consumerGroup = GetValue<string>("consumerGroup");
                    if (consumerGroup == null)
                    {
                        consumerGroup = Microsoft.ServiceBus.Messaging.EventHubConsumerGroup.DefaultGroupName;
                    }

                    var eventProcessorHost = new Microsoft.ServiceBus.Messaging.EventProcessorHost(
                         eventProcessorHostName,
                         eventHubName,
                         consumerGroup,
                         connectionString,
                         storageConnectionString);

                    _eventHubConfiguration.AddEventProcessorHost(eventHubName, eventProcessorHost);
                }
                else
                {
                    attributes.Add(new EventHubAttribute(eventHubName));

                    var client = Microsoft.ServiceBus.Messaging.EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
                    _eventHubConfiguration.AddEventHubClient(eventHubName, client);
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

        private class ServiceBusScriptBinding : ScriptBinding
        {
            public ServiceBusScriptBinding(JObject metadata) : base(metadata)
            {
            }

            public override Collection<Attribute> GetAttributes()
            {
                Collection<Attribute> attributes = new Collection<Attribute>();

                string queueName = GetValue<string>("queueName");
                string topicName = GetValue<string>("topicName");
                string subscriptionName = GetValue<string>("subscriptionName");
                var accessRights = GetEnumValue<Microsoft.ServiceBus.Messaging.AccessRights>("accessRights");

                if (IsTrigger)
                {
                    if (!string.IsNullOrEmpty(topicName) && !string.IsNullOrEmpty(subscriptionName))
                    {
                        attributes.Add(new ServiceBusTriggerAttribute(topicName, subscriptionName, accessRights));
                    }
                    else if (!string.IsNullOrEmpty(queueName))
                    {
                        attributes.Add(new ServiceBusTriggerAttribute(queueName, accessRights));
                    }
                }
                else
                {
                    attributes.Add(new ServiceBusAttribute(queueName ?? topicName, accessRights));
                }

                if (attributes.Count == 0)
                {
                    throw new InvalidOperationException("Invalid ServiceBus trigger configuration.");
                }

                string account = GetValue<string>("connection");
                if (!string.IsNullOrEmpty(account))
                {
                    attributes.Add(new ServiceBusAccountAttribute(account));
                }

                return attributes;
            }

            public override Type DefaultType
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
    }
}
