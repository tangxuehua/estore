namespace EStore
{
    public interface IEventStream
    {
        string AggregateRootId { get; }
        string AggregateRootType { get; }
        int Version { get; }
        string CommandId { get; }
        long Timestamp { get; }
        long CommandCreateTimestamp { get; }
        string Events { get; }
    }
}
