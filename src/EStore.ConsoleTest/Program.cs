using System;
using ECommon.Components;
using ECommon.Configurations;
using ECommon.Utilities;

namespace EStore.ConsoleTest
{
    class Program
    {
        static string _performanceKey = "AppendEvent";
        static IPerformanceService _performanceService;

        static void Main(string[] args)
        {
            var configuration = Configuration
                .Create()
                .UseAutofac()
                .RegisterCommonComponents()
                .UseLog4Net()
                .UseJsonNet()
                .RegisterUnhandledExceptionHandler();

            _performanceService = ObjectContainer.Resolve<IPerformanceService>();
            _performanceService.Initialize(_performanceKey);
            _performanceService.Start();

            AppendEventTest();

            Console.ReadLine();
        }

        static void AppendEventTest()
        {
            var eventStore = new DefaultEventStore();
            eventStore.Load();
            eventStore.Start();
            var eventStream = new EventStream
            {
                SourceId = ObjectId.GenerateNewStringId(),
                Name = "Note",
                Events = "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890",
                CommandId = ObjectId.GenerateNewStringId(),
                Items = string.Empty
            };
            var totalEventCount = 5000 * 10000;

            for (var i = 1; i <= totalEventCount; i++)
            {
                eventStream.Version = i;
                eventStream.Timestamp = DateTime.Now;
                eventStore.AppendStream(eventStream);
                _performanceService.IncrementKeyCount(_performanceKey, (DateTime.Now - eventStream.Timestamp).TotalMilliseconds);
            }

            eventStore.Shutdown();
        }
    }
}
