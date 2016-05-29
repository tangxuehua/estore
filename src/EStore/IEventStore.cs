using System.Collections.Generic;

namespace EStore
{
    public interface IEventStore
    {
        EventAppendStatus AppendStream(EventStream stream);
        EventAppendStatus AppendStreams(IEnumerable<EventStream> streams);
    }
}
