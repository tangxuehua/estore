using System;

namespace EStore.Storage
{
    public class ChunkCompleteException : Exception
    {
        public ChunkCompleteException(string message) : base(message) { }
    }
}
