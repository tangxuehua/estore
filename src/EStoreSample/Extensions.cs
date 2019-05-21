using EStore;
using ECommonConfiguration = ECommon.Configurations.Configuration;

namespace EStoreSample
{
    public static class Extensions
    {
        public static ECommonConfiguration RegisterEStoreComponents(this ECommonConfiguration configuration)
        {
            configuration.SetDefault<IEventStore, DefaultEventStore>();

            return configuration;
        }
    }
}