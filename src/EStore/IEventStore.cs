using System.Collections.Generic;

namespace EStore
{
    public interface IEventStore
    {
        int ChunkCount { get; }
        int MinChunkNum { get; }
        int MaxChunkNum { get; }

        void Load();
        void Start();
        void Shutdown();
        EventAppendStatus AppendStream(EventStream stream);
        EventAppendStatus AppendStreams(IEnumerable<EventStream> streams);
    }
}
