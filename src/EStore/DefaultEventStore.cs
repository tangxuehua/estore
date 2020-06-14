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
        private readonly long SecondFactor = 10000000L;
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
        private readonly SortedDictionary<long, ConcurrentDictionary<string, byte>> _commandIndexDict = new SortedDictionary<long, ConcurrentDictionary<string, byte>>();
        private readonly ConcurrentDictionary<string, AggregateLatestVersionData> _aggregateLatestVersionDict = new ConcurrentDictionary<string, AggregateLatestVersionData>();
        private readonly SortedDictionary<long, ConcurrentDictionary<string, byte>> _aggregateLatestVersionBySecondDict = new SortedDictionary<long, ConcurrentDictionary<string, byte>>();
        private ConcurrentQueue<EventStreamRecord> _eventStreamRecordQueue = new ConcurrentQueue<EventStreamRecord>();
        private ConcurrentQueue<EventStreamRecord> _swapedEventStreamRecordQueue = new ConcurrentQueue<EventStreamRecord>();
        private readonly ManualResetEvent _loadIndexWaitHandle = new ManualResetEvent(false);
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;
        private bool _isEventIndexLoaded = false;
        private bool _isCommandIndexLoaded = false;

        #endregion

        /// <summary>Represents the interval of the persist event index and command index task, the default value is 1 seconds.
        /// </summary>
        public int PersistIndexIntervalMilliseconds { get; set; }
        /// <summary>Represents the max cache time seconds of the command index, the default value is 15 days.
        /// </summary>
        public int CommandIndexMaxCacheSeconds { get; set; }
        /// <summary>Represents the interval of removing command index task, the default value is 10 seconds.
        /// </summary>
        public int RemoveExpiredCommandIndexCacheIntervalSeconds { get; set; }
        /// <summary>Represents the max cache count of the aggregate latest version, the default value is 10000000.
        /// </summary>
        public int AggregateLatestVersionMaxCacheCount { get; set; }
        /// <summary>Represents the interval of removing exceeded aggregate latest version task, the default value is 10 seconds.
        /// </summary>
        public int RemoveExceedAggregateLatestVersionCacheIntervalSeconds { get; set; }

        public DefaultEventStore(IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _scheduleService = scheduleService;
            PersistIndexIntervalMilliseconds = 1000;
            RemoveExpiredCommandIndexCacheIntervalSeconds = 10 * 1000;
            CommandIndexMaxCacheSeconds = 60 * 60 * 24 * 15;
            AggregateLatestVersionMaxCacheCount = 10000000;
            RemoveExceedAggregateLatestVersionCacheIntervalSeconds = 10 * 1000;
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
            _scheduleService.StartTask("RemoveExpiredCommandIndexes", RemoveExpiredCommandIndexes, RemoveExpiredCommandIndexCacheIntervalSeconds, RemoveExpiredCommandIndexCacheIntervalSeconds);
            _scheduleService.StartTask("RemoveExceedAggregates", RemoveExceedAggregateLatestVersion, RemoveExceedAggregateLatestVersionCacheIntervalSeconds, RemoveExceedAggregateLatestVersionCacheIntervalSeconds);
        }
        public void Stop()
        {
            PersistIndex();
            _scheduleService.StopTask("PersistIndex");
            _scheduleService.StopTask("RemoveExpiredCommandIndexes");
            _scheduleService.StopTask("RemoveExceedAggregates");
            _eventDataChunkWriter.Close();
            _eventIndexChunkWriter.Close();
            _commandIndexChunkWriter.Close();
        }
        public EventAppendResult AppendEventStreams(IEnumerable<IEventStream> eventStreams)
        {
            lock (_lockObj)
            {
                var eventStreamDict = GroupEventStreamByAggregateRoot(eventStreams);
                var eventAppendResult = new EventAppendResult();

                foreach (var entry in eventStreamDict)
                {
                    var aggregateRootId = entry.Key;
                    var eventStreamList = entry.Value;

                    //检查当前聚合根的事件是否合法
                    if (!CheckAggregateEvents(aggregateRootId, eventStreamList, eventAppendResult, out AggregateLatestVersionData aggregateLatestVersionData))
                    {
                        continue;
                    }

                    //如果合法，则持久化事件到文件
                    foreach (var eventStream in eventStreamList)
                    {
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

                        //写入事件到文件
                        _eventDataChunkWriter.Write(record);
                         
                        //更新聚合根的内存缓存数据
                        RefreshMemoryCache(aggregateRootId, aggregateLatestVersionData, record);

                        //添加事件到事件队列，以便进行异步持久化事件索引和命令索引
                        _eventStreamRecordQueue.Enqueue(record);
                    }

                    eventAppendResult.SuccessAggregateRootIdList.Add(aggregateRootId);
                }

                return eventAppendResult;
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
                _logger.Info(chunk.ToString());
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
                        _aggregateLatestVersionDict[aggregateRootId] = new AggregateLatestVersionData(long.Parse(itemArray[2]), int.Parse(itemArray[1]));
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

            //检查是否有漏掉的索引没有加载，TODO

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
                _logger.Info(chunk.ToString());
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
                        var cacheKey = BuildSecondCacheKey(commandCreateTimestamp);
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

            //检查是否有漏掉的索引没有加载，TODO

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
            var toRemovePairList = new List<KeyValuePair<long, ConcurrentDictionary<string, byte>>>();
            foreach (var entry in _commandIndexDict)
            {
                if (IsCommandIndexExpired(entry.Key))
                {
                    toRemovePairList.Add(entry);
                }
                else
                {
                    break;
                }
            }

            if (toRemovePairList.Count == 0)
            {
                return;
            }

            var firstPair = toRemovePairList.First();
            var lastPair = toRemovePairList.Last();
            var removedCommandCount = 0;
            foreach (var pair in toRemovePairList)
            {
                if (_commandIndexDict.Remove(pair.Key))
                {
                    removedCommandCount += pair.Value.Count;
                }
            }

            _logger.InfoFormat("Removed commandCacheCount: {0}, firstTimeKey: {1}, lastTimeKey: {2}", removedCommandCount, firstPair.Key, lastPair.Key);
        }
        private void RemoveExceedAggregateLatestVersion()
        {
            var exceedCount = _aggregateLatestVersionDict.Count - AggregateLatestVersionMaxCacheCount;
            if (exceedCount <= 0)
            {
                return;
            }

            var toRemoveAggregateCount = 0;
            var toRemovePairList = new List<KeyValuePair<long, ConcurrentDictionary<string, byte>>>();
            foreach (var entry in _aggregateLatestVersionBySecondDict)
            {
                toRemovePairList.Add(entry);
                toRemoveAggregateCount += entry.Value.Count;
                if (toRemoveAggregateCount >= exceedCount)
                {
                    break;
                }
            }

            var removedAggregateCount = 0;
            foreach (var pair in toRemovePairList)
            {
                _aggregateLatestVersionBySecondDict.Remove(pair.Key);
                foreach (var aggregateRootId in pair.Value.Keys)
                {
                    if (_aggregateLatestVersionDict.TryRemove(aggregateRootId, out AggregateLatestVersionData removed))
                    {
                        removedAggregateCount++;
                    }
                }
            }

            _logger.InfoFormat("Removed aggregateCacheCount: {0}, remainingCount: {1}", removedAggregateCount, _aggregateLatestVersionDict.Count);
        }
        private IDictionary<string, IList<IEventStream>> GroupEventStreamByAggregateRoot(IEnumerable<IEventStream> eventStreams)
        {
            var eventStreamDict = new Dictionary<string, IList<IEventStream>>();
            var aggregateRootIdList = eventStreams.Select(x => x.AggregateRootId).Distinct().ToList();
            foreach (var aggregateRootId in aggregateRootIdList)
            {
                var eventStreamList = eventStreams.Where(x => x.AggregateRootId == aggregateRootId).ToList();
                if (eventStreamList.Count > 0)
                {
                    eventStreamDict.Add(aggregateRootId, eventStreamList);
                }
            }
            return eventStreamDict;
        }
        private bool IsCommandIndexExpired(long key)
        {
            return (DateTime.Now - new DateTime(key * SecondFactor)).TotalSeconds > CommandIndexMaxCacheSeconds;
        }
        private bool CheckAggregateEvents(string aggregateRootId, IList<IEventStream> eventStreamList, EventAppendResult eventAppendResult, out AggregateLatestVersionData aggregateLatestVersionData)
        {
            aggregateLatestVersionData = null;

            //检查版本号是否连续
            if (!IsEventStreamVersionSequential(aggregateRootId, eventStreamList))
            {
                eventAppendResult.DuplicateEventAggregateRootIdList.Add(aggregateRootId);
                return false;
            }

            //检查命令是否重复
            var duplicatedCommandIdList = CheckDuplicatedCommands(aggregateRootId, eventStreamList);
            if (duplicatedCommandIdList.Count > 0)
            {
                eventAppendResult.DuplicateCommandAggregateRootIdList.Add(aggregateRootId, duplicatedCommandIdList);
                return false;
            }

            //检查版本号是否是基于当前聚合根最新的版本号而进行的修改
            if (!CheckAggregateRootLatestVersionMatch(aggregateRootId, eventStreamList.First().Version, out aggregateLatestVersionData))
            {
                eventAppendResult.DuplicateEventAggregateRootIdList.Add(aggregateRootId);
                return false;
            }

            return true;
        }
        private bool IsEventStreamVersionSequential(string aggregateRootId, IList<IEventStream> eventStreamList)
        {
            if (eventStreamList.Count <= 1)
            {
                return true;
            }
            for(var i = 0; i < eventStreamList.Count - 1; i++)
            {
                var version1 = eventStreamList[i].Version;
                var version2 = eventStreamList[i + 1].Version;
                if (version1 + 1 != version2)
                {
                    _logger.ErrorFormat("Aggregate event version sequential wrong，aggregateRootId: {0}, versionPrevious: {1}, versionNext: {2}",
                        aggregateRootId,
                        version1,
                        version2);
                    return false;
                }
            }
            return true;
        }
        private bool CheckAggregateRootLatestVersionMatch(string aggregateRootId, long firstInputEventVersion, out AggregateLatestVersionData aggregateLatestVersionData)
        {
            aggregateLatestVersionData = GetAggregateRootLatestVersionData(aggregateRootId);
            if (aggregateLatestVersionData != null)
            {
                if (aggregateLatestVersionData.Version + 1 != firstInputEventVersion)
                {
                    return false;
                }
            }
            else if (firstInputEventVersion != 1)
            {
                return false;
            }
            return true;
        }
        private AggregateLatestVersionData GetAggregateRootLatestVersionData(string aggregateRootId)
        {
            if (_aggregateLatestVersionDict.TryGetValue(aggregateRootId, out AggregateLatestVersionData aggregateLatestVersionData))
            {
                return aggregateLatestVersionData;
            }
            var aggregateLatestVersionDataFromFile = LoadAggregateRootLatestVersionDataFromDisk(aggregateRootId);
            if (aggregateLatestVersionDataFromFile != null)
            {
                return _aggregateLatestVersionDict.GetOrAdd(aggregateRootId, aggregateLatestVersionDataFromFile);
            }
            return null;
        }
        private void UpdateAggregateByTimeDictCache(string aggregateRootId, AggregateLatestVersionData aggregateLatestVersionData)
        {
            var oldKey = BuildSecondCacheKey(aggregateLatestVersionData.LastActiveTime.Ticks);
            var newKey = BuildSecondCacheKey(DateTime.Now.Ticks);

            //先从旧时间戳的Dict缓存中移除aggregateRootId
            if (_aggregateLatestVersionBySecondDict.TryGetValue(oldKey, out ConcurrentDictionary<string, byte> versionDict1))
            {
                versionDict1.TryRemove(aggregateRootId, out byte removed);
            }

            //再将aggregateRootId添加到新时间戳的Dict缓存下
            if (_aggregateLatestVersionBySecondDict.TryGetValue(newKey, out ConcurrentDictionary<string, byte> versionDict2))
            {
                versionDict2.TryAdd(aggregateRootId, 1);
            }
            else
            {
                var versionDict = new ConcurrentDictionary<string, byte>();
                versionDict.TryAdd(aggregateRootId, 1);
                _aggregateLatestVersionBySecondDict.Add(newKey, versionDict);
            }
        }
        private AggregateLatestVersionData LoadAggregateRootLatestVersionDataFromDisk(string aggregateRootId)
        {
            //TODO
            return null;
        }
        private IList<string> CheckDuplicatedCommands(string aggregateRootId, IList<IEventStream> eventStreamList)
        {
            var duplicatedCommandIdList = new List<string>();
            foreach(var eventStream in eventStreamList)
            {
                if (IsCommandDuplicated(eventStream))
                {
                    duplicatedCommandIdList.Add(eventStream.CommandId);
                }
            }
            return duplicatedCommandIdList;
        }
        private bool IsCommandDuplicated(IEventStream eventStream)
        {
            var commandCacheKey = BuildSecondCacheKey(eventStream.CommandCreateTimestamp);
            if (_commandIndexDict.TryGetValue(commandCacheKey, out ConcurrentDictionary<string, byte> commandIndexDict))
            {
                if (commandIndexDict.ContainsKey(eventStream.CommandId))
                {
                    return true;
                }
            }
            else if (IsCommandIndexExpired(commandCacheKey) && IsCommandExistOnDisk(eventStream))
            {
                return true;
            }
            return false;
        }
        private bool IsCommandExistOnDisk(IEventStream eventStream)
        {
            //TODO
            return false;
        }
        private void RefreshMemoryCache(string aggregateRootId, AggregateLatestVersionData aggregateLatestVersionData, EventStreamRecord record)
        {
            //更新聚合根在本地内存存储的最新的事件版本
            if (aggregateLatestVersionData != null)
            {
                aggregateLatestVersionData.Update(record.LogPosition, record.Version);
                UpdateAggregateByTimeDictCache(aggregateRootId, aggregateLatestVersionData);
            }
            else
            {
                var newAggregateLatestVersionData = new AggregateLatestVersionData(record.LogPosition, record.Version);
                _aggregateLatestVersionDict.TryAdd(record.AggregateRootId, newAggregateLatestVersionData);
                UpdateAggregateByTimeDictCache(aggregateRootId, newAggregateLatestVersionData);
            }

            //添加命令索引到内存字典
            var commandCacheKey = BuildSecondCacheKey(record.CommandCreateTimestamp);
            _commandIndexDict
                .GetOrAdd(commandCacheKey, k => new ConcurrentDictionary<string, byte>())
                .TryAdd(record.CommandId, 1);
        }
        /// <summary>生产一个基于时间戳的缓存key，使用时间戳的截止到秒的时间作为缓存key，秒以下的单位的时间全部清零
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        private long BuildSecondCacheKey(long timestamp)
        {
            return timestamp / SecondFactor;
        }

        class AggregateLatestVersionData
        {
            public long LogPosition { get; private set; }
            public int Version { get; private set; }
            public DateTime LastActiveTime { get; private set; }

            public AggregateLatestVersionData(long logPosition, int version)
            {
                LogPosition = logPosition;
                Version = version;
                LastActiveTime = DateTime.Now;
            }

            public void Update(long logPosition, int version)
            {
                LogPosition = logPosition;
                Version = version;
                LastActiveTime = DateTime.Now;
            }
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
