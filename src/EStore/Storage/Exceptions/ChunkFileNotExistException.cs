using System;

namespace EStore.Storage
{
    public class ChunkFileNotExistException : Exception
    {
        public ChunkFileNotExistException(string fileName) : base(string.Format("Chunk file '{0}' not exist.", fileName)) { }
    }
}
