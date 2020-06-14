using System.Collections.Generic;

namespace EStore
{
    public interface IEventStore
    {
        EventAppendResult AppendEventStreams(IEnumerable<IEventStream> eventStreams);
    }
}
