using System;
using System.Net;
using ECommon.Utilities;

namespace EStore
{
    public class StreamIdUtil
    {
        private static byte[] _ipBytes;
        private static byte[] _portBytes;

        public static string CreateStreamId(IPAddress ipAddress, int port, long streamPosition)
        {
            if (_ipBytes == null)
            {
                _ipBytes = ipAddress.GetAddressBytes();
            }
            if (_portBytes == null)
            {
                _portBytes = BitConverter.GetBytes(port);
            }
            var positionBytes = BitConverter.GetBytes(streamPosition);
            var streamIdBytes = ByteUtil.Combine(_ipBytes, _portBytes, positionBytes);

            return ObjectId.ToHexString(streamIdBytes);
        }
        public static StreamIdInfo ParseStreamId(string streamId)
        {
            var streamIdBytes = ObjectId.ParseHexString(streamId);
            var ipBytes = new byte[4];
            var portBytes = new byte[4];
            var positionBytes = new byte[8];

            Buffer.BlockCopy(streamIdBytes, 0, ipBytes, 0, 4);
            Buffer.BlockCopy(streamIdBytes, 4, portBytes, 0, 4);
            Buffer.BlockCopy(streamIdBytes, 8, positionBytes, 0, 8);

            var port = BitConverter.ToInt32(portBytes, 0);
            var position = BitConverter.ToInt64(positionBytes, 0);

            return new StreamIdInfo
            {
                IP = new IPAddress(ipBytes),
                Port = port,
                StreamPosition = position
            };
        }
    }
    public struct StreamIdInfo
    {
        public IPAddress IP;
        public int Port;
        public long StreamPosition;
    }
}

