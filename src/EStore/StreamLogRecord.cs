using System.IO;
using System.Net;
using EStore.Storage.LogRecords;

namespace EStore
{
    public class StreamLogRecord : EventStream, ILogRecord
    {
        public long LogPosition { get; set; }
        public string StreamId { get; set; }
        public IPAddress IPAddress { get; set; }
        public int Port { get; set; }

        public StreamLogRecord(IPAddress ipAddress, int port)
        {
            IPAddress = ipAddress;
            Port = port;
        }

        public void ReadFrom(byte[] recordBuffer)
        {
            var srcOffset = 0;

            LogPosition = ByteUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            StreamId = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            SourceId = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Name = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Version = ByteUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            Events = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Timestamp = ByteUtils.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
            CommandId = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Items = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
        }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            LogPosition = logPosition;
            StreamId = StreamIdUtil.CreateStreamId(IPAddress, Port, logPosition);

            writer
                .WriteLong(LogPosition)
                .WriteString(StreamId)
                .WriteString(SourceId)
                .WriteString(Name)
                .WriteInt(Version)
                .WriteString(Events)
                .WriteDateTime(Timestamp)
                .WriteString(CommandId)
                .WriteString(Items);
        }
    }
}
