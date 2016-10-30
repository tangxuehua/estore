using System.IO;
using System.Net;
using ECommon.Extensions;
using ECommon.Storage.LogRecords;
using ECommon.Utilities;

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

            LogPosition = ByteUtil.DecodeLong(recordBuffer, srcOffset, out srcOffset);
            StreamId = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
            SourceId = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Name = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Version = ByteUtil.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            Events = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Timestamp = ByteUtil.DecodeDateTime(recordBuffer, srcOffset, out srcOffset);
            CommandId = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Items = ByteUtil.DecodeString(recordBuffer, srcOffset, out srcOffset);
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
