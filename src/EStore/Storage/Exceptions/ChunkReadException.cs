using System;

namespace EStore.Storage
{
    public class ChunkReadException : Exception
    {
        public ChunkReadException(string message) : base(message) { }
    }
}
