using System;
using System.Collections.Generic;
using System.Net;
using ECommon.Components;
using ECommon.Logging;
using ECommon.Storage;
using ECommon.Storage.FileNamingStrategies;

namespace EStore
{
    public class DefaultEventStore : IEventStore
    {
        private readonly object _lockObj = new object();
        private ChunkManager _chunkManager;
        private ChunkWriter _chunkWriter;
        private readonly ILogger _logger;
        private IPAddress _ipAddress = IPAddress.Loopback;   //TODO
        private int _port = 10098;   //TODO

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

        public DefaultEventStore()
        {
            _logger = ObjectContainer.Resolve<ILoggerFactory>().Create(GetType().FullName);
        }

        public void Load()
        {
            var path = @"c:\estore-files\event-chunks";
            var chunkDataSize = 512 * 1024 * 1024;
            var maxLogRecordSize = 4 * 1024 * 1024;
            var config = new ChunkManagerConfig(
                path,
                new DefaultFileNamingStrategy("event-chunk-"),
                chunkDataSize,
                0,
                0,
                100,
                false,
                false,
                Environment.ProcessorCount * 2,
                maxLogRecordSize,
                128 * 1024,
                128 * 1024,
                90,
                45,
                1,
                5,
                1000000,
                false);
            _chunkManager = new ChunkManager("EventChunk", config, false);
            _chunkWriter = new ChunkWriter(_chunkManager);
        }
        public void Start()
        {
            _chunkWriter.Open();
        }
        public void Shutdown()
        {
            _chunkWriter.Close();
            _chunkManager.Close();
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
                return EventAppendStatus.Success;
            }
        }
        public EventAppendStatus AppendStreams(IEnumerable<EventStream> streams)
        {
            //TODO
            return EventAppendStatus.Success;
        }
    }
}
