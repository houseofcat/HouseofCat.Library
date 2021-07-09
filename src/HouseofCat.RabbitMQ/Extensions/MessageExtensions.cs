using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using HouseofCat.RabbitMQ.Pools;
using HouseofCat.Utilities.Random;
using RabbitMQ.Client;

namespace HouseofCat.RabbitMQ
{
    public static class MessageExtensions
    {
        private static readonly XorShift XorShift = new XorShift(true);

        public static TMessage Clone<TMessage>(this IMessage message) 
            where TMessage : IMessage, new()
        {
            return new TMessage
            {
                Envelope = new Envelope
                {
                    Exchange = message.Envelope.Exchange,
                    RoutingKey = message.Envelope.RoutingKey,
                    RoutingOptions = new RoutingOptions
                    {
                        DeliveryMode = message.Envelope.RoutingOptions?.DeliveryMode ?? 2,
                        Mandatory = message.Envelope.RoutingOptions?.Mandatory ?? false,
                        PriorityLevel = message.Envelope.RoutingOptions?.PriorityLevel ?? 0,
                    }
                }
            };
        }

        public static void UpsertHeader(this IMessage message, string key, object value)
        {
            var metadata = message.CreateMetadataIfMissing();
            metadata.UpsertHeader(key, value);
        }
        
        public static void WriteHeadersToMetadata(this IMessage message, IDictionary<string, object> headers)
        {
            var metadata = message.CreateMetadataIfMissing();
            metadata.WriteHeadersToMetadata(headers);
        }
        
        public static IBasicProperties BuildProperties(
            this IMessage message, IChannelHost channelHost, bool withOptionalHeaders) =>
            message.BuildPropertiesAsync(channelHost, withOptionalHeaders).GetAwaiter().GetResult();

        public static IBasicProperties CreateBasicProperties(
            this IMessage message,
            IChannelHost channelHost,
            bool withOptionalHeaders,
            IMetadata metadata) => 
            message.CreateBasicPropertiesAsync(channelHost, withOptionalHeaders, metadata).GetAwaiter().GetResult();

        public static async Task<IBasicProperties> CreateBasicPropertiesAsync(
            this IMessage message,
            IChannelHost channelHost,
            bool withOptionalHeaders,
            IMetadata metadata)
        {
            var channel = await channelHost.GetChannelAsync().ConfigureAwait(false);
            var props = channel.CreateBasicProperties();

            props.DeliveryMode = message.Envelope.RoutingOptions.DeliveryMode;
            props.ContentType = message.Envelope.RoutingOptions.MessageType;
            props.Priority = message.Envelope.RoutingOptions.PriorityLevel;
            props.MessageId = message.MessageId ?? Guid.NewGuid().ToString();

            if (!props.IsHeadersPresent())
            {
                props.Headers = new Dictionary<string, object>();
            }

            if (withOptionalHeaders && metadata != null)
            {
                foreach (var kvp in metadata?.CustomFields)
                {
                    if (kvp.Key.StartsWith(Constants.HeaderPrefix, StringComparison.OrdinalIgnoreCase))
                    {
                        props.Headers[kvp.Key] = kvp.Value;
                    }
                }
            }
            
            return props;
        }

        public static IMessage CreateSimpleRandomLetter(string queueName, int bodySize = 1000)
        {
            var payload = new byte[bodySize];
            XorShift.FillBuffer(payload, 0, bodySize);

            return new Letter
            {
                MessageId = Guid.NewGuid().ToString(),
                LetterMetadata = new LetterMetadata(),
                Envelope = new Envelope
                {
                    Exchange = string.Empty,
                    RoutingKey = queueName,
                    RoutingOptions = new RoutingOptions
                    {
                        DeliveryMode = 1,
                        PriorityLevel = 0
                    }
                },
                Body = payload
            };
        }

        public static IList<IMessage> CreateManySimpleRandomLetters(List<string> queueNames, int letterCount, int bodySize = 1000)
        {
            var random = new Random();
            var letters = new List<IMessage>();

            var queueCount = queueNames.Count;
            for (int i = 0; i < letterCount; i++)
            {
                letters.Add(CreateSimpleRandomLetter(queueNames[random.Next(0, queueCount)], bodySize));
            }

            return letters;
        }

        public static IList<IMessage> CreateManySimpleRandomLetters(string queueName, int letterCount, int bodySize = 1000)
        {
            var letters = new List<IMessage>();

            for (int i = 0; i < letterCount; i++)
            {
                letters.Add(CreateSimpleRandomLetter(queueName, bodySize));
            }

            return letters;
        }
    }
}
