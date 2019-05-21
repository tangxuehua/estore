using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Storage;
using ECommon.Utilities;

namespace EStore
{
    public class DefaultEventStore : IEventStore
    {
        #region Private Variables

        private readonly object _lockObj = new object();
        private const char IndexSeparator = ';';
        private const char IndexContentSeparator = ':';
        private const int PersistIndexBatchSize = 1000;
        private ChunkManager _eventDataChunkManager;
        private ChunkWriter _eventDataChunkWriter;
        private ChunkReader _eventDataChunkReader;
        private ChunkManager _eventIndexChunkManager;
        private ChunkWriter _eventIndexChunkWriter;
        private ChunkReader _eventIndexChunkReader;
        private ChunkManager _commandIndexChunkManager;
        private ChunkWriter _commandIndexChunkWriter;
        private ChunkReader _commandIndexChunkReader;
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<string, byte>> _commandIndexDict = new ConcurrentDictionary<long, ConcurrentDictionary<string, byte>>();
        private readonly ConcurrentDictionary<string, AggregateLatestVersionData> _aggregateLatestVersionDict = new ConcurrentDictionary<string, AggregateLatestVersionData>();
        private ConcurrentQueue<EventStreamRecord> _eventStreamRecordQueue = new ConcurrentQueue<EventStreamRecord>();
        private ConcurrentQueue<EventStreamRecord> _swapedEventStreamRecordQueue = new ConcurrentQueue<EventStreamRecord>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private bool _isEventIndexLoaded = false;
        private bool _isCommandIndexLoaded = false;
        private readonly ManualResetEvent _loadIndexWaitHandle = new ManualResetEvent(false);

        #endregion

        /// <summary>Represents the interval of the persist event index and command index task, the default value is 1 seconds.
        /// </summary>
        public int PersistIndexIntervalMilliseconds { get; set; }
        /// <summary>Represents the max cache time seconds of the command index, the default value is 3 days.
        /// </summary>
        public int MaxCommandIndexCacheTimeSeconds { get; set; }
        /// <summary>Represents the interval of remove command index task, the default value is 10 seconds.
        /// </summary>
        public int RemoveExpiredCommandIndexCacheKeyIntervalSeconds { get; set; }

        public DefaultEventStore(IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _scheduleService = scheduleService;
            PersistIndexIntervalMilliseconds = 1000;
            RemoveExpiredCommandIndexCacheKeyIntervalSeconds = 10 * 1000;
            MaxCommandIndexCacheTimeSeconds = 60 * 60 * 24 * 3;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Init(ChunkManagerConfig eventDataChunkConfig, ChunkManagerConfig eventIndexChunkConfig, ChunkManagerConfig commandIndexChunkConfig)
        {
            _eventDataChunkManager = new ChunkManager("EventDataChunk", eventDataChunkConfig, false);
            _eventDataChunkWriter = new ChunkWriter(_eventDataChunkManager);
            _eventDataChunkReader = new ChunkReader(_eventDataChunkManager, _eventDataChunkWriter);

            _eventIndexChunkManager = new ChunkManager("EventIndexChunk", eventIndexChunkConfig, false);
            _eventIndexChunkWriter = new ChunkWriter(_eventIndexChunkManager);
            _eventIndexChunkReader = new ChunkReader(_eventIndexChunkManager, _eventIndexChunkWriter);

            _commandIndexChunkManager = new ChunkManager("CommandIndexChunk", commandIndexChunkConfig, false);
            _commandIndexChunkWriter = new ChunkWriter(_commandIndexChunkManager);
            _commandIndexChunkReader = new ChunkReader(_commandIndexChunkManager, _commandIndexChunkWriter);

            _eventDataChunkManager.Load(ReadEventStreamRecord);
            _eventIndexChunkManager.Load(ReadIndexRecord);
            _commandIndexChunkManager.Load(ReadIndexRecord);

            _eventDataChunkWriter.Open();
            _eventIndexChunkWriter.Open();
            _commandIndexChunkWriter.Open();

            LoadIndexData();
        }
        public void Start()
        {
            _scheduleService.StartTask("PersistIndex", PersistIndex, PersistIndexIntervalMilliseconds, PersistIndexIntervalMilliseconds);
            _scheduleService.StartTask("RemoveExpiredCommandIndexes", RemoveExpiredCommandIndexes, RemoveExpiredCommandIndexCacheKeyIntervalSeconds, RemoveExpiredCommandIndexCacheKeyIntervalSeconds);
        }
        public void Stop()
        {
            _scheduleService.StopTask("PersistIndex");
            _scheduleService.StopTask("RemoveExpiredCommandIndexes");
            _eventDataChunkWriter.Close();
        }
        public EventAppendResult AppendEventStream(IEventStream eventStream)
        {
            lock (_lockObj)
            {
                //判断命令是否重复
                var commandCacheKey = BuildCommandCacheKey(eventStream.CommandCreateTimestamp);
                if (_commandIndexDict.TryGetValue(commandCacheKey, out ConcurrentDictionary<string, byte> commandIndexDict))
                {
                    if (commandIndexDict.ContainsKey(eventStream.CommandId))
                    {
                        return EventAppendResult.DuplicateCommand;
                    }
                }

                //判断版本号是否冲突，使用内存中仅保留每个聚合根最新版本的字典来实现
                if (_aggregateLatestVersionDict.TryGetValue(eventStream.AggregateRootId, out AggregateLatestVersionData aggregateLatestVersionData))
                {
                    if (eventStream.Version != (aggregateLatestVersionData.Version + 1))
                    {
                        return EventAppendResult.InvalidEventVersion;
                    }
                }
                else if (eventStream.Version != 1)
                {
                    return EventAppendResult.InvalidEventVersion;
                }

                //写入eventStream到文件
                var record = new EventStreamRecord
                {
                    AggregateRootId = eventStream.AggregateRootId,
                    AggregateRootType = eventStream.AggregateRootType,
                    Version = eventStream.Version,
                    CommandId = eventStream.CommandId,
                    Timestamp = eventStream.Timestamp,
                    CommandCreateTimestamp = eventStream.CommandCreateTimestamp,
                    Events = eventStream.Events
                };
                if (aggregateLatestVersionData != null)
                {
                    record.PreviousRecordLogPosition = aggregateLatestVersionData.LogPosition;
                }
                _eventDataChunkWriter.Write(record);

                //更新聚合根最新的事件版本
                if (aggregateLatestVersionData != null)
                {
                    aggregateLatestVersionData.Version = record.Version;
                    aggregateLatestVersionData.LogPosition = record.LogPosition;
                }
                else
                {
                    aggregateLatestVersionData = new AggregateLatestVersionData
                    {
                        Version = record.Version,
                        LogPosition = record.LogPosition
                    };
                    _aggregateLatestVersionDict[eventStream.AggregateRootId] = aggregateLatestVersionData;
                }

                //添加命令索引到内存字典
                _commandIndexDict
                    .GetOrAdd(commandCacheKey, k => new ConcurrentDictionary<string, byte>())
                    .TryAdd(eventStream.CommandId, 1);

                //添加事件到事件队列，以便进行异步持久化事件索引和命令索引
                _eventStreamRecordQueue.Enqueue(record);

                return EventAppendResult.Success;
            }
        }

        private void LoadIndexData()
        {
            Task.Factory.StartNew(LoadEventIndexData);
            Task.Factory.StartNew(LoadCommandIndexData);
            _loadIndexWaitHandle.WaitOne();
        }
        private void LoadEventIndexData()
        {
            var chunks = _eventIndexChunkManager.GetAllChunks();
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
                    var record = _eventIndexChunkReader.TryReadAt(dataPosition, ReadIndexRecord, false);
                    if (record == null)
                    {
                        break;
                    }
                    if (string.IsNullOrEmpty(record.IndexInfo))
                    {
                        continue;
                    }
                    var indexArray = record.IndexInfo.Split(IndexSeparator);
                    foreach (var index in indexArray)
                    {
                        if (string.IsNullOrEmpty(index))
                        {
                            continue;
                        }
                        var itemArray = index.Split(IndexContentSeparator);
                        var aggregateRootId = itemArray[0];
                        _aggregateLatestVersionDict[aggregateRootId] = new AggregateLatestVersionData
                        {
                            Version = int.Parse(itemArray[1]),
                            LogPosition = long.Parse(itemArray[2])
                        };
                        count++;
                        if (count % 1000000 == 0)
                        {
                            _logger.Info("Event index loaded: " + count);
                        }
                    }
                    dataPosition += record.RecordSize + 4 + 4;
                }
                _logger.Info("Event index chunk read complete, timeSpent: " + stopWatch.Elapsed.TotalSeconds );
            }
            _logger.Info("Event index chunk read all complete, total timeSpent: " + totalStopWatch.Elapsed.TotalSeconds);
            _isEventIndexLoaded = true;
            if (_isCommandIndexLoaded)
            {
                _loadIndexWaitHandle.Set();
            }
        }
        private void LoadCommandIndexData()
        {
            var chunks = _commandIndexChunkManager.GetAllChunks();
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
                    var record = _commandIndexChunkReader.TryReadAt(dataPosition, ReadIndexRecord, false);
                    if (record == null)
                    {
                        break;
                    }
                    if (string.IsNullOrEmpty(record.IndexInfo))
                    {
                        continue;
                    }
                    var indexArray = record.IndexInfo.Split(IndexSeparator);
                    foreach (var index in indexArray)
                    {
                        if (string.IsNullOrEmpty(index))
                        {
                            continue;
                        }
                        var array = index.Split(IndexContentSeparator);
                        if (array.Length != 3)
                        {
                            continue;
                        }
                        var commandId = array[0];
                        var commandCreateTimestamp = long.Parse(array[1]);
                        var cacheKey = BuildCommandCacheKey(commandCreateTimestamp);
                        if (!IsCommandIndexExpired(cacheKey))
                        {
                            _commandIndexDict
                                .GetOrAdd(cacheKey, k => new ConcurrentDictionary<string, byte>())
                                .TryAdd(commandId, 1);
                            count++;
                            if (count % 1000000 == 0)
                            {
                                _logger.Info("Command index loaded: " + count);
                            }
                        }
                    }
                    dataPosition += record.RecordSize + 4 + 4;
                }
                _logger.Info("Command index chunk read complete, timeSpent: " + stopWatch.Elapsed.TotalSeconds);
            }
            _logger.Info("Command index chunk read all complete, total timeSpent: " + totalStopWatch.Elapsed.TotalSeconds);
            _isCommandIndexLoaded = true;
            if (_isEventIndexLoaded)
            {
                _loadIndexWaitHandle.Set();
            }
        }
        private EventStreamRecord ReadEventStreamRecord(byte[] recordBuffer)
        {
            var record = new EventStreamRecord();
            record.ReadFrom(recordBuffer);
            return record;
        }
        private IndexRecord ReadIndexRecord(byte[] recordBuffer)
        {
            var record = new IndexRecord();
            record.ReadFrom(recordBuffer);
            return record;
        }
        private void PersistIndex()
        {
            lock (_lockObj)
            {
                var tmp = _eventStreamRecordQueue;
                _eventStreamRecordQueue = _swapedEventStreamRecordQueue;
                _swapedEventStreamRecordQueue = tmp;
            }

            var eventIndexBuilder = new StringBuilder();
            var commandIndexBuilder = new StringBuilder();
            var count = 0;
            while (_swapedEventStreamRecordQueue.TryDequeue(out EventStreamRecord eventStreamRecord))
            {
                eventIndexBuilder.Append(eventStreamRecord.AggregateRootId);
                eventIndexBuilder.Append(IndexContentSeparator);
                eventIndexBuilder.Append(eventStreamRecord.Version);
                eventIndexBuilder.Append(IndexContentSeparator);
                eventIndexBuilder.Append(eventStreamRecord.Timestamp);
                eventIndexBuilder.Append(IndexContentSeparator);
                eventIndexBuilder.Append(eventStreamRecord.LogPosition);
                eventIndexBuilder.Append(IndexSeparator);

                commandIndexBuilder.Append(eventStreamRecord.CommandId);
                commandIndexBuilder.Append(IndexContentSeparator);
                commandIndexBuilder.Append(eventStreamRecord.CommandCreateTimestamp);
                commandIndexBuilder.Append(IndexContentSeparator);
                commandIndexBuilder.Append(eventStreamRecord.LogPosition);
                commandIndexBuilder.Append(IndexSeparator);

                count++;

                if (count % PersistIndexBatchSize == 0)
                {
                    _eventIndexChunkWriter.Write(new IndexRecord
                    {
                        IndexInfo = eventIndexBuilder.ToString()
                    });
                    _commandIndexChunkWriter.Write(new IndexRecord
                    {
                        IndexInfo = commandIndexBuilder.ToString()
                    });
                    eventIndexBuilder.Clear();
                    commandIndexBuilder.Clear();
                }
            }
            if (eventIndexBuilder.Length > 0)
            {
                _eventIndexChunkWriter.Write(new IndexRecord
                {
                    IndexInfo = eventIndexBuilder.ToString()
                });
            }
            if (commandIndexBuilder.Length > 0)
            {
                _commandIndexChunkWriter.Write(new IndexRecord
                {
                    IndexInfo = commandIndexBuilder.ToString()
                });
            }
        }
        private void RemoveExpiredCommandIndexes()
        {
            _commandIndexDict
                .Keys
                .Where(x => IsCommandIndexExpired(x))
                .ToList()
                .ForEach(key =>
                {
                    if (_commandIndexDict.TryRemove(key, out ConcurrentDictionary<string, byte> value))
                    {
                        _logger.InfoFormat("Removed expired command index: timeKey:{0}, commandId count: {1}", key, value.Keys.Count);
                    }
                });
        }
        /// <summary>生成命令产生时间的时间戳的缓存key，使用时间戳的截止到秒的时间作为缓存key，秒以下的单位的时间全部清零
        /// </summary>
        /// <param name="commandCreateTimestamp"></param>
        /// <returns></returns>
        private long BuildCommandCacheKey(long commandCreateTimestamp)
        {
            return commandCreateTimestamp / 1000000;
        }
        private bool IsCommandIndexExpired(long key)
        {
            return (DateTime.Now - new DateTime(key * 1000000)).TotalSeconds > MaxCommandIndexCacheTimeSeconds;
        }

        class AggregateLatestVersionData
        {
            public long LogPosition { get; set; }
            public int Version { get; set; }
        }
        class EventStreamRecord : ILogRecord
        {
            public long PreviousRecordLogPosition { get; set; }
            public long LogPosition { get; set; }
            public long RecordSize { get; set; }
            public string AggregateRootId { get; set; }
            public string AggregateRootType { get; set; }
            public int Version { get; set; }
            public string CommandId { get; set; }
            public long CommandCreateTimestamp { get; set; }
            public long Timestamp { get; set; }
            public string Events { get; set; }

            public void ReadFrom(byte[] recordBuffer)
            {
                var srcOffset = 0;

                LogPosition = ByteUtil.DecodeLong(recordBuffer, srcOffset, out srcOffset);
                PreviousRecordLogPosition = ByteUtil.DecodeLong(recordBuffer, srcOffset, out srcOffset);
                AggregateRootId = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
                AggregateRootType = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
                Version = ByteUtil.DecodeInt(recordBuffer, srcOffset, out srcOffset);
                CommandId = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
                CommandCreateTimestamp = ByteUtil.DecodeLong(recordBuffer, srcOffset, out srcOffset);
                Timestamp = ByteUtil.DecodeLong(recordBuffer, srcOffset, out srcOffset);
                Events = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);

                RecordSize = recordBuffer.Length;
            }
            public void WriteTo(long logPosition, BinaryWriter writer)
            {
                LogPosition = logPosition;

                //LogPosition
                writer.Write(LogPosition);

                //PreviousRecordLogPosition
                writer.Write(PreviousRecordLogPosition);

                //AggregateRootId
                var aggregateRootIdBytes = Encoding.UTF8.GetBytes(AggregateRootId);
                writer.Write(aggregateRootIdBytes.Length);
                writer.Write(aggregateRootIdBytes);

                //AggregateRootType
                var aggregateRootTypeBytes = Encoding.UTF8.GetBytes(AggregateRootType);
                writer.Write(aggregateRootTypeBytes.Length);
                writer.Write(aggregateRootTypeBytes);

                //Version
                writer.Write(Version);

                //CommandId
                var commandIdBytes = Encoding.UTF8.GetBytes(CommandId);
                writer.Write(commandIdBytes.Length);
                writer.Write(commandIdBytes);

                //CommandCreateTimestamp
                writer.Write(CommandCreateTimestamp);

                //Timestamp
                writer.Write(Timestamp);

                //Events
                var eventsBytes = Encoding.UTF8.GetBytes(Events);
                writer.Write(eventsBytes.Length);
                writer.Write(eventsBytes);
            }
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
