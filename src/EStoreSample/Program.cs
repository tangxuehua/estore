using System;
using System.Collections.Generic;
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

            var chunkFileStoreRootPath = @"d:\event-store";
            var eventDataChunkDataSize = 256 * 1024 * 1024;
            var indexChunkDataSize = 256 * 1024 * 1024;
            var chunkFlushInterval = 100;
            var chunkCacheMaxCount = 15;
            var chunkCacheMinCount = 15;
            var maxDataLogRecordSize = 5 * 1024 * 1024;
            var maxIndexLogRecordSize = 5 * 1024 * 1024;
            var chunkWriteBuffer = 128 * 1024;
            var chunkReadBuffer = 128 * 1024;
            var syncFlush = false;
            var flushOption = FlushOption.FlushToOS;
            var enableCache = true;
            var preCacheChunkCount = 10;
            var chunkLocalCacheSize = 300000;

            var eventDataChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, "event-data-chunks"),
                new DefaultFileNamingStrategy("event-data-chunk-"),
                eventDataChunkDataSize,
                0,
                0,
                chunkFlushInterval,
                enableCache,
                syncFlush,
                flushOption,
                Environment.ProcessorCount * 8,
                maxDataLogRecordSize,
                chunkWriteBuffer,
                chunkReadBuffer,
                chunkCacheMaxCount,
                chunkCacheMinCount,
                preCacheChunkCount,
                5,
                chunkLocalCacheSize,
                true);
            var eventIndexChunkConfig = new ChunkManagerConfig(
                Path.Combine(chunkFileStoreRootPath, "event-index-chunks"),
                new DefaultFileNamingStrategy("event-index-chunk-"),
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
                chunkLocalCacheSize,
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
                chunkLocalCacheSize,
                true);

            var eventStore = ObjectContainer.Resolve<IEventStore>() as DefaultEventStore;

            eventStore.Init(eventDataChunkConfig, eventIndexChunkConfig, commandIndexChunkConfig);
            eventStore.Start();

            var totalCount = 100000000;
            var count = 0;
            var performanceService = ObjectContainer.Resolve<IPerformanceService>();
            performanceService.Initialize("WriteChunk").Start();

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        var start = DateTime.Now;
                        var current = 0;
                        var eventStreamList = new List<IEventStream>();
                        for (var i = 0; i < 10; i++)
                        {
                            current = Interlocked.Increment(ref count);
                            var eventStream = new TestEventStream
                            {
                                AggregateRootId = "123456789012345678901234",
                                AggregateRootType = "Note",
                                Events = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
                                Timestamp = DateTime.UtcNow.Ticks,
                                Version = current,
                                CommandId = ObjectId.GenerateNewStringId(),
                                CommandCreateTimestamp = DateTime.Now.Ticks
                            };
                            eventStreamList.Add(eventStream);
                            performanceService.IncrementKeyCount("default", (DateTime.Now - start).TotalMilliseconds);
                        }
                        eventStore.AppendEventStreams(eventStreamList);
                        if (current > totalCount)
                        {
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                    }
                }
            });

            Console.ReadLine();
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
