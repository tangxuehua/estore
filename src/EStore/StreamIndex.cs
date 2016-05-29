using System.IO;
using EStore.Storage.LogRecords;

namespace EStore
{
    public class ScopeIndex
    {
        public string SourceId { get; set; }
        public int StartPosition { get; set; }
        public int EndPosition { get; set; }
    }
    public class StreamIndex : ILogRecord
    {
        public string SourceId { get; set; }
        public string CommandId { get; set; }
        public int Version { get; set; }
        public long BodyPosition { get; set; }

        public void WriteTo(long logPosition, BinaryWriter writer)
        {
            writer
                .WriteString(SourceId)
                .WriteString(CommandId)
                .WriteInt(Version)
                .Write(BodyPosition);
        }

        public void ReadFrom(byte[] recordBuffer)
        {
            var srcOffset = 0;

            SourceId = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            CommandId = ByteUtils.DecodeString(recordBuffer, srcOffset, out srcOffset);
            Version = ByteUtils.DecodeInt(recordBuffer, srcOffset, out srcOffset);
            BodyPosition = ByteUtils.DecodeLong(recordBuffer, srcOffset, out srcOffset);
        }
    }
}
