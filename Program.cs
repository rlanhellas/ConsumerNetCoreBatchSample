using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using Confluent.Kafka;
using Lanhellas.KafkaConsumerBatch;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Extensions.Logging;

namespace ConsumerBatchSample
{
    class Program
    {
        static void Main(string[] args)
        {
            
            //constants
            const string topic = "throughput-beta2-lib-test";
            const int maxRecords = 1000000;
            const int batchSize = 500;
            
            //config consumer
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "TYPE_HERE_YOUR_KAFKA_SERVER_ADDRESS",
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = "YOUR_USERNAME",
                SaslPassword = "YOUR_PASSWORD",
                GroupId = "consumer-sample-batch-beta2",
                ClientId = $"consumer-sample-batch-beta2-{Dns.GetHostName()}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false
            };
            
            //control cancel token
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, eventArgs) =>
            {
                eventArgs.Cancel = true;
                cts.Cancel();
            };

            //create logger
            var serilogLogger = new LoggerConfiguration().WriteTo.Console().MinimumLevel.Information().CreateLogger();
            var logger = new SerilogLoggerProvider(serilogLogger).CreateLogger("consumerKafka");
            
            //create consumer
            using var consumer = new ConsumerBuilder<string,string>(consumerConfig).Build();
            consumer.Subscribe(topic);
            
            //create batch wrapper
            var batch = KafkaConsumerBatchBuilder<string, string>.Config()
                .WithConsumer(consumer)
                .WithLogger(logger)
                .WithBatchSize(batchSize)
                .WithMaxWaitTime(TimeSpan.FromSeconds(1))
                .Build();

            var stopWatch = Stopwatch.StartNew();
            var countTotalRecords = 0;

            logger.LogInformation("Consumer Started");
               
            try
            {
                while (countTotalRecords < maxRecords && !cts.IsCancellationRequested)
                {
                    var results = batch.ConsumeBatch();
                    if (results.Count <= 0) continue;
                    
                    countTotalRecords += results.Count;
                    logger.LogInformation("Commit Results {size}, CountTotal {count}", results.Count, countTotalRecords);
                    consumer.Commit();
                }
            }
            catch (OperationCanceledException e)
            {
                //
            }
            finally
            {
                consumer.Close();
                logger.LogInformation("Consumer Stoped");
            }
            
            stopWatch.Stop();
            var totalTime = stopWatch.Elapsed;
            logger.LogInformation("Total Time Spent {spent}", totalTime);
        }
    }
}