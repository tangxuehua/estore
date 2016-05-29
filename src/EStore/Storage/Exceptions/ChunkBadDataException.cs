using System;

namespace EStore.Storage
{
    public class ChunkBadDataException : Exception
    {
        public ChunkBadDataException(string message) : base(message)
        {
        }
    }
}
