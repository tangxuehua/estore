using System.Threading.Tasks;

namespace EStore
{
    public interface IEventStore
    {
        EventAppendResult AppendEventStream(IEventStream eventStream);
    }
}
