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

            var eventStore = InitEventStore();

            var totalCount = 100000000;
            var index = 0;
            var performanceService = ObjectContainer.Resolve<IPerformanceService>();
            performanceService.Initialize("WriteChunk").Start();
            var aggregateRootId = ObjectId.GenerateNewStringId();
            var aggregateRootType = "Note";
            var events = "1234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890";

            Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    try
                    {
                        var start = DateTime.Now;
                        var currentVersion = 0;
                        var eventStreamList = new List<IEventStream>();
                        for (var i = 0; i < 5; i++)
                        {
                            currentVersion = Interlocked.Increment(ref index);
                            var eventStream = new TestEventStream
                            {
                                AggregateRootId = aggregateRootId,
                                AggregateRootType = aggregateRootType,
                                Events = events,
                                Timestamp = DateTime.UtcNow.Ticks,
                                Version = currentVersion,
                                CommandId = ObjectId.GenerateNewStringId(),
                                CommandCreateTimestamp = DateTime.Now.Ticks
                            };
                            eventStreamList.Add(eventStream);
                            performanceService.IncrementKeyCount("default", (DateTime.Now - start).TotalMilliseconds);
                        }
                        eventStore.AppendEventStreams(eventStreamList);
                        if (index > totalCount)
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
        static DefaultEventStore InitEventStore()
        {
            var chunkFileStoreRootPath = @"d:\event-store";
            var eventDataChunkDataSize = 1024 * 1024 * 1024;
            var indexChunkDataSize = 256 * 1024 * 1024;
            var chunkFlushInterval = 100;
            var chunkCacheMaxCount = 1;
            var chunkCacheMinCount = 1;
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

            eventStore.Init(eventDataChunkConfig, aggregateIndexChunkConfig, commandIndexChunkConfig);
            eventStore.Start();

            return eventStore;
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
