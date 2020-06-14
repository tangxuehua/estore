using System.Collections.Generic;

namespace EStore
{
    public class EventAppendResult
    {
        /// <summary>成功的聚合根
        /// </summary>
        public IList<string> SuccessAggregateRootIdList { get; private set; }
        /// <summary>事件版本号有冲突的聚合根
        /// </summary>
        public IList<string> DuplicateEventAggregateRootIdList { get; private set; }
        /// <summary>命令有重复的聚合根
        /// </summary>
        public IDictionary<string, IList<string>> DuplicateCommandAggregateRootIdList { get; private set; }

        public EventAppendResult()
        {
            SuccessAggregateRootIdList = new List<string>();
            DuplicateEventAggregateRootIdList = new List<string>();
            DuplicateCommandAggregateRootIdList = new Dictionary<string, IList<string>>();
        }
    }
}
