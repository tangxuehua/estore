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
    public class DefaultEventVersionStore
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

        public DefaultEventVersionStore(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.Create(GetType().FullName);
        }

        public bool AppendVersion(EventStream stream)
        {
            return true;
            //lock (_lockObj)
            //{
            //    var record = new StreamLogRecord(_ipAddress, _port)
            //    {
            //        SourceId = stream.SourceId,
            //        Name = stream.Name,
            //        Version = stream.Version,
            //        Events = stream.Events,
            //        CommandId = stream.CommandId,
            //        Timestamp = stream.Timestamp,
            //        Items = stream.Items
            //    };
            //    _chunkWriter.Write(record);

            //    var dict = _commandDict.GetOrAdd(stream.SourceId, x => new ConcurrentDictionary<string, byte>());
            //    if (!dict.TryAdd(stream.CommandId, 1))
            //    {
            //        return EventAppendStatus.DuplicateCommand;
            //    }

            //    //TODO
            //    return EventAppendStatus.Success;
            //}
        }
    }
}
