namespace EStore
{
    public enum EventAppendResult
    {
        Success = 1,
        Failed = 2,
        InvalidEventVersion = 3,
        DuplicateCommand = 4
    }
}
