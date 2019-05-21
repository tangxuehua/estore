namespace EStore
{
    public interface ICommandIdManager
    {
        bool IsCommandIdExist(CommandInfo commandInfo);
        void AddCommandId(CommandInfo commandInfo);
    }
}
