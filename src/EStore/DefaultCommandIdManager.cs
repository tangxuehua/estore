using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Globalization;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Storage;
using ECommon.Utilities;
using System.IO;
using System.Text;
using System.Diagnostics;
using System.Collections.Generic;

namespace EStore
{
    /// <summary>命令管理器，内部会自动清理已过期的命令；支持可配置最多保留多久的命令。
    /// </summary>
    public class DefaultCommandIdManager : ICommandIdManager
    {
        private const string TimeFormat = "yyyyMMddHHmmss";
        private readonly IScheduleService _scheduleService;
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _commandIndexDict = new ConcurrentDictionary<long, ConcurrentDictionary<string, byte>>();
        private readonly ILogger _logger;
        private ChunkManager _indexChunkManager;
        private ChunkWriter _indexChunkWriter;
        private ChunkReader _indexChunkReader;
        private readonly object _lockObj = new object();
        private readonly char Separator = ':';
        private ConcurrentQueue<CommandInfo> _changedCommandIndexQueue = new ConcurrentQueue<CommandInfo>();
        private ConcurrentQueue<CommandInfo> _swappedCommandIndexQueue = new ConcurrentQueue<CommandInfo>();

        public int MaxCacheTimeSeconds { get; set; }
        public int RemoveExpiredCacheKeyIntervalSeconds { get; set; }
        public int PersistIndexIntervalMilliseconds { get; set; }

        public DefaultCommandIdManager(IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _scheduleService = scheduleService;
            _logger = loggerFactory.Create(GetType().FullName);
            RemoveExpiredCacheKeyIntervalSeconds = 10 * 1000;
            PersistIndexIntervalMilliseconds = 1000;
            MaxCacheTimeSeconds = 60 * 60 * 24 * 3;
        }

        public void Init(ChunkManagerConfig indexChunkConfig)
        {
            _indexChunkManager = new ChunkManager("CommandIndexChunk", indexChunkConfig, false);
            _indexChunkWriter = new ChunkWriter(_indexChunkManager);
            _indexChunkReader = new ChunkReader(_indexChunkManager, _indexChunkWriter);
            _indexChunkManager.Load(ReadIndexRecord);
            _indexChunkWriter.Open();
            LoadCommandIndexData();
        }
        public void Start()
        {
            _scheduleService.StartTask("PersistIndex", PersistIndex, PersistIndexIntervalMilliseconds, PersistIndexIntervalMilliseconds);
            _scheduleService.StartTask("RemoveExpiredKeys", RemoveExpiredKeys, RemoveExpiredCacheKeyIntervalSeconds, RemoveExpiredCacheKeyIntervalSeconds);
        }
        public void Stop()
        {
            _scheduleService.StopTask("PersistIndex");
            _scheduleService.StopTask("RemoveExpiredKeys");
        }

        public bool IsCommandIdExist(CommandInfo commandInfo)
        {
            var key = GetCacheKey(commandInfo.CommandCreateTimestamp);
            if (_commandIndexDict.TryGetValue(key, out ConcurrentDictionary<string, byte> dict))
            {
                return dict.ContainsKey(commandInfo.CommandId);
            }
            return false;
        }
        public void AddCommandId(CommandInfo commandInfo)
        {
            lock (_lockObj)
            {
                var key = GetCacheKey(commandInfo.CommandCreateTimestamp);
                _commandIndexDict
                    .GetOrAdd(key, k => new ConcurrentDictionary<string, byte>())
                    .TryAdd(commandInfo.CommandId, 1);
                _changedCommandIndexQueue.Enqueue(commandInfo);
            }
        }

        private void LoadCommandIndexData()
        {
            var chunks = _indexChunkManager.GetAllChunks();
            var count = 0L;
            var stopWatch = new Stopwatch();
            var totalStopWatch = new Stopwatch();
            totalStopWatch.Start();
            foreach (Chunk chunk in chunks)
            {
                stopWatch.Restart();
                _logger.Info(chunk);
                var dataPosition = chunk.ChunkHeader.ChunkDataStartPosition;
                var recordQueue = new Queue<IndexRecord>();
                while (true)
                {
                    var record = _indexChunkReader.TryReadAt(dataPosition, ReadIndexRecord, false);
                    if (record == null)
                    {
                        break;
                    }
                    if (string.IsNullOrEmpty(record.IndexInfo))
                    {
                        continue;
                    }
                    var indexArray = record.IndexInfo.Split(';');
                    foreach (var index in indexArray)
                    {
                        if (string.IsNullOrEmpty(index))
                        {
                            continue;
                        }
                        var array = index.Split(Separator);
                        if (array.Length != 2)
                        {
                            continue;
                        }
                        var commandId = array[0];
                        var commandCreateTimestamp = long.Parse(array[1]);
                        var cacheKey = GetCacheKey(commandCreateTimestamp);
                        if (!IsExpired(cacheKey))
                        {
                            AddCommandId(new CommandInfo
                            {
                                CommandId = commandId,
                                CommandCreateTimestamp = commandCreateTimestamp
                            });
                            count++;
                            if (count % 100000 == 0)
                            {
                                _logger.Info(count);
                            }
                        }
                    }
                    dataPosition += record.RecordSize + 4 + 4;
                }
                _logger.Info("Chunk read complete, timeSpent: " + stopWatch.Elapsed.TotalSeconds);
            }
            _logger.Info("Chunk read all complete, total timeSpent: " + totalStopWatch.Elapsed.TotalSeconds);
        }
        long totalPersistedIndexCount = 0L;
        private void PersistIndex()
        {
            lock (_lockObj)
            {
                var tmp = _changedCommandIndexQueue;
                _changedCommandIndexQueue = _swappedCommandIndexQueue;
                _swappedCommandIndexQueue = tmp;
            }

            var batchSize = 1000;
            var builder = new StringBuilder();
            var count = 0;
            while (_swappedCommandIndexQueue.TryDequeue(out CommandInfo commandInfo))
            {
                builder.Append(commandInfo.CommandId + Separator + commandInfo.CommandCreateTimestamp);
                builder.Append(";");
                count++;
                totalPersistedIndexCount++;
                if (count % batchSize == 0)
                {
                    _indexChunkWriter.Write(new IndexRecord
                    {
                        IndexInfo = builder.ToString()
                    });
                    builder.Clear();
                }
            }
            if (builder.Length > 0)
            {
                _indexChunkWriter.Write(new IndexRecord
                {
                    IndexInfo = builder.ToString()
                });
            }
            _logger.InfoFormat("Persisted command indexes, count: {0}, totalPersistedIndexCount: {1}", count, totalPersistedIndexCount);
        }
        private IndexRecord ReadIndexRecord(byte[] recordBuffer)
        {
            var record = new IndexRecord();
            record.ReadFrom(recordBuffer);
            return record;
        }
        private void RemoveExpiredKeys()
        {
            _commandIndexDict
                .Keys
                .Where(x => IsExpired(x))
                .ToList()
                .ForEach(key =>
                {
                    if (_commandIndexDict.TryRemove(key, out ConcurrentDictionary<string, byte> value))
                    {
                        _logger.InfoFormat("Removed expired command time key, time: {0}", key);
                    }
                });
        }
        private long GetCacheKey(long commandCreateTimestamp)
        {
            return commandCreateTimestamp / 1000000;
        }
        private bool IsExpired(long key)
        {
            return (DateTime.Now - new DateTime(key * 1000000)).TotalSeconds > MaxCacheTimeSeconds;
        }

        class IndexRecord : ILogRecord
        {
            public long RecordSize { get; set; }
            public string IndexInfo { get; set; }

            public void ReadFrom(byte[] recordBuffer)
            {
                var srcOffset = 0;
                IndexInfo = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
                RecordSize = recordBuffer.Length;
            }
            public void WriteTo(long logPosition, BinaryWriter writer)
            {
                var indexInfoBytes = Encoding.UTF8.GetBytes(IndexInfo);
                writer.Write(indexInfoBytes.Length);
                writer.Write(indexInfoBytes);
            }
        }
    }
}
