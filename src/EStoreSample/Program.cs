using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Storage;
using ECommon.Storage.FileNamingStrategies;
using ECommon.Utilities;
using EStore;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace EStoreSample
{
    class Program
    {
        static void Main(string[] args)
        {
            AddAggregateEventTest();
        }

        static void AddAggregateEventTest()
        {
            ECommonConfiguration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .RegisterUnhandledExceptionHandler()
                .RegisterEStoreComponents()
                .BuildContainer();

            string chunkFileStoreRootPath = @"d:\event-store";
            int messageChunkDataSize = 256 * 1024 * 1024;
            int indexChunkDataSize = 256 * 1024 * 1024;
            int chunkFlushInterval = 100;
            int chunkCacheMaxCount = 15;
            int chunkCacheMinCount = 15;
            int maxLogRecordSize = 5 * 1024 * 1024;
            int maxIndexLogRecordSize = 5 * 1024 * 1024;
            int chunkWriteBuffer = 128 * 1024;
            int chunkReadBuffer = 128 * 1024;
            bool syncFlush = false;
            FlushOption flushOption = FlushOption.FlushToOS;
            bool enableCache = true;
            var preCacheChunkCount = 10;
            int messageChunkLocalCacheSize = 300000;

            var chunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, "aggregate-event-chunks"),
                new DefaultFileNamingStrategy("aggregate-event-chunk-"),
                messageChunkDataSize,
                0,
                0,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                flushOption,
                Environment.ProcessorCount * 8,
                maxLogRecordSize,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxCount,
                chunkCacheMinCount,
                preCacheChunkCount,
                5,
                messageChunkLocalCacheSize,
                true);
            var aggregateIndexChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, "aggregate-index-chunks"),
                new DefaultFileNamingStrategy("aggregate-index-chunk-"),
                indexChunkDataSize,
                0,
                0,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                flushOption,
                Environment.ProcessorCount * 8,
                maxIndexLogRecordSize,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxCount,
                chunkCacheMinCount,
                preCacheChunkCount,
                5,
                messageChunkLocalCacheSize,
                true);
            var commandIndexChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, "command-index-chunks"),
                new DefaultFileNamingStrategy("command-index-chunk-"),
                indexChunkDataSize,
                0,
                0,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                flushOption,
                Environment.ProcessorCount * 8,
                maxIndexLogRecordSize,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxCount,
                chunkCacheMinCount,
                preCacheChunkCount,
                5,
                messageChunkLocalCacheSize,
                true);

            var commandManager = ObjectContainer.Resolve<ICommandIdManager>() as DefaultCommandIdManager;
            var eventStore = ObjectContainer.Resolve<IEventStore>() as DefaultEventStore;

            commandManager.Init(commandIndexChunkConfig);
            commandManager.Start();
            eventStore.Init(chunkConfig, aggregateIndexChunkConfig);
            eventStore.Start();

            var eventStream = new TestEventStream
            {
                AggregateRootId = ObjectId.GenerateNewStringId(),
                AggregateRootType = "Note",
                Events = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
                Timestamp = DateTime.UtcNow.Ticks
            };

            var totalCount = 10000000;
            var count = 0;
            var performanceService = ObjectContainer.Resolve<IPerformanceService>();
            performanceService.Initialize("WriteChunk").Start();

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        var current = Interlocked.Increment(ref count);
                        if (current > totalCount)
                        {
                            break;
                        }
                        var start = DateTime.Now;
                        eventStream.Version = current;
                        eventStream.CommandId = ObjectId.GenerateNewStringId();
                        eventStream.CommandCreateTimestamp = DateTime.Now.Ticks;
                        eventStore.AppendEventStream(eventStream);
                        performanceService.IncrementKeyCount("default", (DateTime.Now - start).TotalMilliseconds);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            });

            Console.ReadLine();
            commandManager.Stop();
            eventStore.Stop();
        }

        class TestEventStream : IEventStream
        {
            public string AggregateRootId { get; set; }
            public string AggregateRootType { get; set; }
            public int Version { get; set; }
            public string CommandId { get; set; }
            public long Timestamp { get; set; }
            public long CommandCreateTimestamp { get; set; }
            public string Events { get; set; }
        }
    }
}
