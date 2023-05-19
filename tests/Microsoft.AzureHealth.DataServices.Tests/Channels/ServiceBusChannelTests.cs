﻿using System;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;
using Microsoft.AzureHealth.DataServices.Channels;
using Microsoft.AzureHealth.DataServices.Tests.Assets;
using Microsoft.AzureHealth.DataServices.Tests.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json;
using Serilog;

namespace Microsoft.AzureHealth.DataServices.Tests.Channels
{
    [TestClass]
    public class ServiceBusChannelTests
    {
        private static readonly string LogPath = "../../servicebuslog.txt";
        private static ServiceBusConfig config;
        private static Microsoft.Extensions.Logging.ILogger<ServiceBusChannel> logger;

        public ServiceBusChannelTests()
        {
        }

        [ClassInitialize]
        public static void Initialize(TestContext context)
        {
            Console.WriteLine(context.TestName);
            IConfigurationBuilder builder = new ConfigurationBuilder();
            builder.AddUserSecrets(Assembly.GetExecutingAssembly(), true);
            builder.AddEnvironmentVariables("PROXY_");
            IConfigurationRoot root = builder.Build();
            config = new ServiceBusConfig();
            root.Bind(config);

            Serilog.Core.Logger slog = new LoggerConfiguration()
            .WriteTo.File(
            LogPath,
            shared: true,
            flushToDiskInterval: TimeSpan.FromMilliseconds(10000))
            .MinimumLevel.Debug()
            .CreateLogger();

            Microsoft.Extensions.Logging.ILoggerFactory factory = LoggerFactory.Create(log =>
            {
                log.SetMinimumLevel(LogLevel.Trace);
                log.AddConsole();
                log.AddSerilog(slog);
            });

            logger = factory.CreateLogger<ServiceBusChannel>();
            factory.Dispose();

            Console.WriteLine(context.TestName);
        }

        [TestCleanup]
        public async Task CleanupTest()
        {
            ServiceBusClient client = new(config.ServiceBusConnectionString);
            ServiceBusReceiver subscriptionReceiver = client.CreateReceiver(config.ServiceBusTopic, config.ServiceBusSubscription);

            while (await subscriptionReceiver.PeekMessageAsync() != null)
            {
                ServiceBusReceivedMessage msg = await subscriptionReceiver.ReceiveMessageAsync();
                await subscriptionReceiver.CompleteMessageAsync(msg);
            }

            ServiceBusReceiver queueReceiver = client.CreateReceiver(config.ServiceBusQueue);

            while (await queueReceiver.PeekMessageAsync() != null)
            {
                ServiceBusReceivedMessage msg = await queueReceiver.ReceiveMessageAsync();
                await queueReceiver.CompleteMessageAsync(msg);
            }
        }

        [TestInitialize]
        public async Task InitialTest()
        {
            ServiceBusClient client = new(config.ServiceBusConnectionString);
            ServiceBusReceiver subscriptionReceiver = client.CreateReceiver(config.ServiceBusTopic, config.ServiceBusSubscription);

            while (await subscriptionReceiver.PeekMessageAsync() != null)
            {
                ServiceBusReceivedMessage msg = await subscriptionReceiver.ReceiveMessageAsync();
                await subscriptionReceiver.CompleteMessageAsync(msg);
            }

            ServiceBusReceiver queueReceiver = client.CreateReceiver(config.ServiceBusQueue);

            while (await queueReceiver.PeekMessageAsync() != null)
            {
                ServiceBusReceivedMessage msg = await queueReceiver.ReceiveMessageAsync();
                await queueReceiver.CompleteMessageAsync(msg);
            }
        }

        [TestMethod]
        public async Task ServiceBusChannel_TopicSendSmallMessage_Test()
        {
            string contentType = "application/json";
            string propertyName = "Key1";
            string value = "Value";
            string contentString = $"{{ \"{propertyName}\": \"{value}\" }}";
            byte[] message = Encoding.UTF8.GetBytes(contentString);

            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                FallbackStorageConnectionString = config.ServiceBusBlobConnectionString,
                FallbackStorageContainer = config.ServiceBusBlobContainer,
                Topic = config.ServiceBusTopic,
                Subscription = config.ServiceBusSubscription,
            });

            IChannel channel = new ServiceBusChannel(options);
            channel.OnError += (a, args) =>
            {
                Assert.Fail($"Channel error {args.Error.Message}");
            };

            bool completed = false;

            channel.OnReceive += (a, args) =>
            {
                string actual = Encoding.UTF8.GetString(args.Message);
                Assert.AreEqual(contentString, actual, "Content mismatch.");
                completed = true;
            };

            await channel.OpenAsync();
            await channel.SendAsync(message, new object[] { contentType });
            await channel.ReceiveAsync();
            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsTrue(completed, "Did not complete.");
        }

        [TestMethod]
        public async Task ServiceBusChannel_TopicSendLargeMessage_Test()
        {
            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                FallbackStorageConnectionString = config.ServiceBusBlobConnectionString,
                FallbackStorageContainer = config.ServiceBusBlobContainer,
                Topic = config.ServiceBusTopic,
                Subscription = config.ServiceBusSubscription,
            });

            LargeJsonMessage msg = new();
            msg.Load(10, 300000);
            string json = JsonConvert.SerializeObject(msg);

            string contentType = "application/json";
            byte[] message = Encoding.UTF8.GetBytes(json);
            IChannel channel = new ServiceBusChannel(options, logger);
            Exception error = null;
            channel.OnError += (a, args) =>
            {
                error = args.Error;
            };

            bool completed = false;
            channel.OnReceive += (a, args) =>
            {
                try
                {
                    string actual = Encoding.UTF8.GetString(args.Message);
                    LargeJsonMessage actualMsg = JsonConvert.DeserializeObject<LargeJsonMessage>(actual);

                    Assert.AreEqual(msg.Fields[0].Value, actualMsg.Fields[0].Value, "Content mismatch.");
                    completed = true;
                }
                catch (Exception ex)
                {
                    error = ex;
                }
            };

            await channel.OpenAsync();
            await channel.SendAsync(message, new object[] { contentType });
            await channel.ReceiveAsync();

            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsNull(error, "Error {0}-{1}", error?.Message, error?.StackTrace);
            Assert.IsTrue(completed, "Did not detect OnReceive event.");
        }

        [TestMethod]
        public async Task ServiceBusChannel_QueueSendSmallMessage_Test()
        {
            string contentType = "application/json";
            string propertyName = "Key1";
            string value = "Value";
            string contentString = $"{{ \"{propertyName}\": \"{value}\" }}";
            byte[] message = Encoding.UTF8.GetBytes(contentString);

            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                FallbackStorageConnectionString = config.ServiceBusBlobConnectionString,
                FallbackStorageContainer = config.ServiceBusBlobContainer,
                Queue = config.ServiceBusQueue,
            });

            IChannel channel = new ServiceBusChannel(options);
            channel.OnError += (a, args) =>
            {
                Assert.Fail($"Channel error {args.Error.Message}");
            };

            bool completed = false;

            channel.OnReceive += (a, args) =>
            {
                string actual = Encoding.UTF8.GetString(args.Message);
                Assert.AreEqual(contentString, actual, "Content mismatch.");
                completed = true;
            };

            await channel.OpenAsync();
            await channel.SendAsync(message, new object[] { contentType });
            await channel.ReceiveAsync();
            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsTrue(completed, "Did not complete.");
        }

        [TestMethod]
        public async Task ServiceBusChannel_QueueSendLargeMessage_Test()
        {
            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                FallbackStorageConnectionString = config.ServiceBusBlobConnectionString,
                FallbackStorageContainer = config.ServiceBusBlobContainer,
                Queue = config.ServiceBusQueue,
            });

            LargeJsonMessage msg = new();
            msg.Load(10, 300000);
            string json = JsonConvert.SerializeObject(msg);

            string contentType = "application/json";
            byte[] message = Encoding.UTF8.GetBytes(json);
            IChannel channel = new ServiceBusChannel(options, logger);
            Exception error = null;
            channel.OnError += (a, args) =>
            {
                error = args.Error;
            };

            bool completed = false;
            channel.OnReceive += (a, args) =>
            {
                try
                {
                    string actual = Encoding.UTF8.GetString(args.Message);
                    LargeJsonMessage actualMsg = JsonConvert.DeserializeObject<LargeJsonMessage>(actual);

                    Assert.AreEqual(msg.Fields[0].Value, actualMsg.Fields[0].Value, "Content mismatch.");
                    completed = true;
                }
                catch (Exception ex)
                {
                    error = ex;
                }
            };

            await channel.OpenAsync();
            await channel.SendAsync(message, new object[] { contentType });
            await channel.ReceiveAsync();

            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsNull(error, "Error {0}-{1}", error?.Message, error?.StackTrace);
            Assert.IsTrue(completed, "Did not detect OnReceive event.");
        }

        [TestMethod]
        public async Task ServiceBusChannel_QueueSendViaServiceBusAndReceiveViaChannelSmallMessage()
        {
            string contentType = "application/json";
            string propertyName = "Key1";
            string value = "Value";
            string contentString = $"{{ \"{propertyName}\": \"{value}\" }}";
            byte[] message = Encoding.UTF8.GetBytes(contentString);

            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                Queue = config.ServiceBusQueue,
            });

            IChannel channel = new ServiceBusChannel(options);
            channel.OnError += (a, args) =>
            {
                Assert.Fail($"Channel error {args.Error.Message}");
            };

            bool completed = false;

            channel.OnReceive += (a, args) =>
            {
                string actual = Encoding.UTF8.GetString(args.Message);
                Assert.AreEqual(contentString, actual, "Content mismatch.");
                completed = true;
            };

            await channel.OpenAsync();
            await channel.ReceiveAsync();

            ServiceBusClient client = new ServiceBusClient(config.ServiceBusConnectionString);
            ServiceBusSender sender = client.CreateSender(config.ServiceBusQueue);
            ServiceBusMessage msg = new(message);
            msg.ContentType = contentType;
            await sender.SendMessageAsync(msg);

            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsTrue(completed, "Did not complete.");
        }

        [TestMethod]
        public async Task ServiceBusChannel_TopicSendViaServiceBusAndReceiveViaChannelSmallMessage()
        {
            string contentType = "application/json";
            string propertyName = "Key1";
            string value = "Value";
            string contentString = $"{{ \"{propertyName}\": \"{value}\" }}";
            byte[] message = Encoding.UTF8.GetBytes(contentString);

            IOptions<ServiceBusChannelOptions> options = Options.Create<ServiceBusChannelOptions>(new ServiceBusChannelOptions()
            {
                ConnectionString = config.ServiceBusConnectionString,
                Sku = config.ServiceBusSku,
                Topic = config.ServiceBusTopic,
                Subscription = config.ServiceBusSubscription,
            });

            IChannel channel = new ServiceBusChannel(options);
            channel.OnError += (a, args) =>
            {
                Assert.Fail($"Channel error {args.Error.Message}");
            };

            bool completed = false;

            channel.OnReceive += (a, args) =>
            {
                string actual = Encoding.UTF8.GetString(args.Message);
                Assert.AreEqual(contentString, actual, "Content mismatch.");
                completed = true;
            };

            await channel.OpenAsync();
            await channel.ReceiveAsync();

            ServiceBusClient client = new ServiceBusClient(config.ServiceBusConnectionString);
            ServiceBusSender sender = client.CreateSender(config.ServiceBusTopic);
            ServiceBusMessage msg = new(message);
            msg.ContentType = contentType;
            await sender.SendMessageAsync(msg);

            int i = 0;
            while (!completed && i < 10)
            {
                await Task.Delay(1000);
                i++;
            }

            channel.Dispose();
            Assert.IsTrue(completed, "Did not complete.");
        }
    }
}
