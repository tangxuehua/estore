using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Configurations;
using ECommon.Utilities;
using EStore.Storage;

namespace EStore.ConsoleTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var configuration = Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler();

            configuration.SetDefault<IChunkStatisticService, DefaultChunkStatisticService>();

            var root = @"c:\estore-files";
            var path = Path.Combine(root, @"stream-index-chunks");
            var singleFileIndexCount = 100 * 10000;
            var dataUnitSize = 68;
            var indexCountForSingleAgg = 10;
            var totalAggNumber = 50 * 10000;

            var config = new ChunkManagerConfig(
                path,
                new DefaultFileNamingStrategy("stream-index-"),
                0,
                dataUnitSize,
                singleFileIndexCount,
                100,
                false,
                false,
                Environment.ProcessorCount * 2,
                12,
                128 * 1024,
                128 * 1024,
                90,
                45,
                1,
                5,
                1000000,
                false,
                false);
            var chunkManager = new ChunkManager("default", config);

            CreateChunks(root, path, chunkManager, singleFileIndexCount, dataUnitSize, totalAggNumber, indexCountForSingleAgg);
            ReadAggIndexTest(chunkManager, singleFileIndexCount, dataUnitSize, indexCountForSingleAgg);

            Console.ReadLine();
        }

        static void CreateChunks(string root, string path, ChunkManager chunkManager, int singleFileIndexCount, int dataUnitSize, int totalAggNumber, int indexCountForSingleAgg)
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, true);
            }
            Directory.CreateDirectory(root);

            if (Directory.Exists(path))
            {
                Directory.Delete(path, true);
            }
            Directory.CreateDirectory(path);

            var chunkWriter = new ChunkWriter(chunkManager);

            chunkWriter.Open();

            var watch = Stopwatch.StartNew();
            var format = "{0}{1:00000000000000}";
            var aggPrefix = "aggregate_";

            for (var i = 0; i < totalAggNumber; i++)
            {
                var sourceId = string.Format(format, aggPrefix, i);
                for (var j = 0; j < indexCountForSingleAgg; j++)
                {
                    var record = new StreamIndex()
                    {
                        SourceId = sourceId,
                        CommandId = ObjectId.GenerateNewStringId(),
                        Version = j + 1,
                        BodyPosition = 10000L
                    };
                    chunkWriter.Write(record);
                }
            }

            chunkWriter.Close();

            Console.WriteLine("Create event index success, aggCount: {0}, totalEventIndexCount: {1}, timeSpent: {2}ms", totalAggNumber, totalAggNumber * indexCountForSingleAgg, watch.ElapsedMilliseconds);
        }
        static void ReadAggIndexTest(ChunkManager chunkManager, int singleFileIndexCount, int dataUnitSize, int indexCountForSingleAgg)
        {
            var queryCount = 1000;
            var parallelLevel = 4;

            chunkManager.Load(x =>
            {
                var index = new StreamIndex();
                index.ReadFrom(x);
                return index;
            });

            var waitHandle = new ManualResetEvent(false);
            var totalExpectCount = parallelLevel * queryCount * indexCountForSingleAgg;
            var totalFoundCount = 0;
            var watch = Stopwatch.StartNew();

            for (var i = 0; i < parallelLevel; i++)
            {
                Task.Factory.StartNew(() =>
                {
                    var baseAggNumber = new Random().Next(98000);
                    var foundCount = ReadIndex(chunkManager, dataUnitSize, singleFileIndexCount, indexCountForSingleAgg, baseAggNumber, queryCount);
                    var currentCount = Interlocked.Add(ref totalFoundCount, foundCount);
                    if (currentCount == totalExpectCount)
                    {
                        waitHandle.Set();
                    }
                });
            }
            waitHandle.WaitOne();

            Console.WriteLine("Read single chunk success, parallelLevel: {0}, aggCount: {1}, timeSpent: {2}ms", parallelLevel, totalFoundCount / indexCountForSingleAgg, watch.ElapsedMilliseconds);

        }
        static int ReadIndex(ChunkManager chunkManager, int dataUnitSize, int singleFileIndexCount, int eventIndexOfEachAgg, int baseAggNumber, int queryCount)
        {
            var format = "{0}{1:00000000000000}";
            var aggPrefix = "aggregate_";
            var foundCount = 0;

            for (var aggNumber = baseAggNumber; aggNumber < (baseAggNumber + queryCount); aggNumber++)
            {
                var position = aggNumber * eventIndexOfEachAgg * dataUnitSize;
                var chunk = chunkManager.GetChunkFor(position);
                var expectSourceId = string.Format(format, aggPrefix, aggNumber);
                var indexList = new List<StreamIndex>();
                var start = chunk.ChunkHeader.GetLocalDataPosition(position);

                for (var i = start; i < singleFileIndexCount * dataUnitSize; i += dataUnitSize)
                {
                    try
                    {
                        var streamIndex = chunk.TryReadAt(i, x =>
                        {
                            var index = new StreamIndex();
                            index.ReadFrom(x);
                            return index;
                        }, false);
                        if (streamIndex.SourceId == expectSourceId)
                        {
                            indexList.Add(streamIndex);
                        }
                        else if (indexList.Count > 0)
                        {
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(string.Format("i: {0}, ex: {1}", i, ex));
                        break;
                    }
                }

                foundCount += indexList.Count;
            }

            return foundCount;
        }
    }
}
