using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using ECommon.Logging;
using EStore.Storage;

namespace EStore
{
    public class StreamSource
    {
        private HashSet<string> _commandIdSet = new HashSet<string>();
        private HashSet<int> _versionSet = new HashSet<int>();
        private int _maxVersion;

        public EventAppendStatus AppendStreamIndex(EventStream stream)
        {
            if (stream.Version > _maxVersion + 1)
            {
                throw new Exception(string.Format("Unexpected event stream version, sourceId: {0}, expect version: {1}, but was {2}", stream.SourceId, _maxVersion + 1, stream.Version));
            }
            if (stream.Version <= _maxVersion)
            {
                return EventAppendStatus.DuplicateEvent;
            }
            if (_commandIdSet.Contains(stream.CommandId))
            {
                return EventAppendStatus.DuplicateCommand;
            }

            _versionSet.Add(stream.Version);
            _commandIdSet.Add(stream.CommandId);

            return EventAppendStatus.Success;
        }
    }
    public class DefaultEventStore : IEventStore
    {
        private ChunkManager _chunkManager;
        private ChunkWriter _chunkWriter;
        private readonly ILogger _logger;
        private readonly object _lockObj = new object();
        private IPAddress _ipAddress;
        private int _port;
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, Byte>> _commandDict = new ConcurrentDictionary<string, ConcurrentDictionary<string, byte>>();

        public long MinMessagePosition
        {
            get
            {
                return _chunkManager.GetFirstChunk().ChunkHeader.ChunkDataStartPosition;
            }
        }
        public long CurrentMessagePosition
        {
            get
            {
                return _chunkWriter.CurrentChunk.GlobalDataPosition;
            }
        }
        public int ChunkCount
        {
            get { return _chunkManager.GetChunkCount(); }
        }
        public int MinChunkNum
        {
            get { return _chunkManager.GetFirstChunk().ChunkHeader.ChunkNumber; }
        }
        public int MaxChunkNum
        {
            get { return _chunkManager.GetLastChunk().ChunkHeader.ChunkNumber; }
        }

        public DefaultEventStore(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public EventAppendStatus AppendStream(EventStream stream)
        {
            lock (_lockObj)
            {
                var record = new StreamLogRecord(_ipAddress, _port)
                {
                    SourceId = stream.SourceId,
                    Name = stream.Name,
                    Version = stream.Version,
                    Events = stream.Events,
                    CommandId = stream.CommandId,
                    Timestamp = stream.Timestamp,
                    Items = stream.Items
                };
                _chunkWriter.Write(record);

                var dict = _commandDict.GetOrAdd(stream.SourceId, x => new ConcurrentDictionary<string, byte>());
                if (!dict.TryAdd(stream.CommandId, 1))
                {
                    return EventAppendStatus.DuplicateCommand;
                }

                //TODO
                return EventAppendStatus.Success;
            }
        }
        public EventAppendStatus AppendStreams(IEnumerable<EventStream> streams)
        {
            return EventAppendStatus.Success;
            //lock (_lockObj)
            //{
            //    var record = new StreamLogRecord(
            //    message.Topic,
            //    message.Code,
            //    message.Body,
            //    queueId,
            //    queueOffset,
            //    message.CreatedTime,
            //    DateTime.Now,
            //    message.Tag);
            //    _chunkWriter.Write(record);
            //    return record;
            //}|
        }
    }
}
