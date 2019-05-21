using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using ECommon.Logging;
using ECommon.Scheduling;
using ECommon.Storage;
using ECommon.Utilities;

namespace EStore
{
    public class DefaultEventStore : IEventStore
    {
        private ChunkManager _chunkManager;
        private ChunkWriter _chunkWriter;
        private ChunkReader _chunkReader;
        private ChunkManager _indexChunkManager;
        private ChunkWriter _indexChunkWriter;
        private ChunkReader _indexChunkReader;
        private readonly object _lockObj = new object();
        private ConcurrentQueue<string> _changedAggregateQueue = new ConcurrentQueue<string>();
        private ConcurrentQueue<string> _swappedAggregateQueue = new ConcurrentQueue<string>();
        private readonly ICommandIdManager _commandIdManager;
        private readonly ConcurrentDictionary<string, AggregateLatestVersionData> _aggregateLatestVersionDict = new ConcurrentDictionary<string, AggregateLatestVersionData>();
        private readonly IScheduleService _scheduleService;
        private readonly ILogger _logger;

        /// <summary>Represents the interval of the persist aggregate index task, the default value is 1 seconds.
        /// </summary>
        public int PersistIndexIntervalMilliseconds { get; set; }

        public DefaultEventStore(ICommandIdManager commandIdManager, IScheduleService scheduleService, ILoggerFactory loggerFactory)
        {
            _commandIdManager = commandIdManager;
            _scheduleService = scheduleService;
            PersistIndexIntervalMilliseconds = 1000;
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public void Init(ChunkManagerConfig eventChunkConfig, ChunkManagerConfig indexChunkConfig)
        {
            _chunkManager = new ChunkManager("AggregateEventChunk", eventChunkConfig, false);
            _chunkWriter = new ChunkWriter(_chunkManager);
            _chunkReader = new ChunkReader(_chunkManager, _chunkWriter);
            _indexChunkManager = new ChunkManager("AggregateIndexChunk", indexChunkConfig, false);
            _indexChunkWriter = new ChunkWriter(_indexChunkManager);
            _indexChunkReader = new ChunkReader(_indexChunkManager, _indexChunkWriter);
            _chunkManager.Load(ReadEventStreamRecord);
            _indexChunkManager.Load(ReadIndexRecord);
            _chunkWriter.Open();
            _indexChunkWriter.Open();
            LoadAggregateVersionIndexData();
        }
        public void Start()
        {
            _scheduleService.StartTask("PersistIndex", PersistIndex, PersistIndexIntervalMilliseconds, PersistIndexIntervalMilliseconds);
        }
        public void Stop()
        {
            _scheduleService.StopTask("PersistIndex");
            _chunkWriter.Close();
        }
        public EventAppendResult AppendEventStream(IEventStream eventStream)
        {
            lock (_lockObj)
            {
                //从CommandId管理器判断命令是否重复
                var commandInfo = new CommandInfo { CommandId = eventStream.CommandId, CommandCreateTimestamp = eventStream.CommandCreateTimestamp };
                if (_commandIdManager.IsCommandIdExist(commandInfo))
                {
                    return EventAppendResult.DuplicateCommand;
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
                    Events = eventStream.Events
                };
                if (aggregateLatestVersionData != null)
                {
                    record.PreviousRecordLogPosition = aggregateLatestVersionData.LogPosition;
                }
                _chunkWriter.Write(record);

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

                //添加CommandId到命令管理器
                _commandIdManager.AddCommandId(commandInfo);

                //更新最近已修改的聚合根ID
                _changedAggregateQueue.Enqueue(record.AggregateRootId);

                return EventAppendResult.Success;
            }   
        }

        private void LoadAggregateVersionIndexData()
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
                        var itemArray = index.Split(':');
                        var aggregateRootId = itemArray[0];
                        _aggregateLatestVersionDict[aggregateRootId] = new AggregateLatestVersionData
                        {
                            Version = int.Parse(itemArray[1]),
                            LogPosition = long.Parse(itemArray[2])
                        };
                        count++;
                        if (count % 100000 == 0)
                        {
                            _logger.Info(count);
                        }
                    }
                    dataPosition += record.RecordSize + 4 + 4;
                }
                _logger.Info("Chunk read complete, timeSpent: " + stopWatch.Elapsed.TotalSeconds );
            }
            _logger.Info("Chunk read all complete, total timeSpent: " + totalStopWatch.Elapsed.TotalSeconds);
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
        long totalPersistedIndexCount = 0L;
        private void PersistIndex()
        {
            lock (_lockObj)
            {
                var tmp = _changedAggregateQueue;
                _changedAggregateQueue = _swappedAggregateQueue;
                _swappedAggregateQueue = tmp;
            }

            var batchSize = 1000;
            var builder = new StringBuilder();
            var count = 0;
            while (_swappedAggregateQueue.TryDequeue(out string aggregateRootId))
            {
                if (!_aggregateLatestVersionDict.TryGetValue(aggregateRootId, out AggregateLatestVersionData aggregateLatestVersionData))
                {
                    continue;
                }

                builder.Append(aggregateRootId);
                builder.Append(":");
                builder.Append(aggregateLatestVersionData.Version);
                builder.Append(":");
                builder.Append(aggregateLatestVersionData.LogPosition);
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
            _logger.InfoFormat("Persisted aggregate indexes, count: {0}, totalPersistedIndexCount: {1}", count, totalPersistedIndexCount);
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
