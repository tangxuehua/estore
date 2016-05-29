namespace EStore
{
    public interface IEventVersionStore
    {
        bool AppendStreamVersion(EventStream stream);
    }
}
