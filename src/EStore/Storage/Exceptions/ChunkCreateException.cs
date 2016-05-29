using System;

namespace EStore.Storage
{
    public class ChunkCreateException : Exception
    {
        public ChunkCreateException(string message) : base(message) { }
    }
}
