using System;

namespace EStore
{
    /// <summary>Represents an event stream.
    /// </summary>
    [Serializable]
    public class EventStream
    {
        /// <summary>The source id of the event stream.
        /// </summary>
        public string SourceId { get; set; }
        /// <summary>The name of the event stream.
        /// </summary>
        public string Name { get; set; }
        /// <summary>The version of the event stream.
        /// </summary>
        public int Version { get; set; }
        /// <summary>The inner events of the event stream.
        /// </summary>
        public string Events { get; set; }
        /// <summary>The timestamp of the event stream.
        /// </summary>
        public DateTime Timestamp { get; set; }
        /// <summary>The command id of the event stream.
        /// </summary>
        public string CommandId { get; set; }
        /// <summary>The extra items of the event stream.
        /// </summary>
        public string Items { get; set; }
    }
}
