using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using ECommon.Logging;

namespace EStore
{
    public class CacheDict<TKey, TValue> where TValue : ITimeValue
    {
        private readonly object _lockObj = new object();
        private readonly long SecondFactor = 10000000L;
        private readonly Dictionary<TKey, TValue> _dataDict = new Dictionary<TKey, TValue>();
        private readonly Dictionary<long, Dictionary<TKey, byte>> _timeDataDict = new Dictionary<long, Dictionary<TKey, byte>>();
        private readonly string _name;
        private readonly ILogger _logger;
        private readonly int _maxCacheCount;

        public CacheDict(string name, ILogger logger, int maxCacheCount)
        {
            _name = name;
            _logger = logger;
            _maxCacheCount = maxCacheCount;
        }

        public int Count
        {
            get
            {
                lock (_lockObj)
                {
                    return _dataDict.Count;
                }
            }
        }
        public bool Exist(TKey key)
        {
            lock (_lockObj)
            {
                return _dataDict.ContainsKey(key);
            }
        }
        public TValue Get(TKey key)
        {
            lock (_lockObj)
            {
                if (_dataDict.TryGetValue(key, out TValue value))
                {
                    return value;
                }
                return default;
            }
        }
        public void AddOrUpdate(TKey key, TValue value)
        {
            lock (_lockObj)
            {
                if (_dataDict.TryGetValue(key, out TValue existing))
                {
                    UpdateTimeKeyDict(key, existing.LastActiveTimestamp, value.LastActiveTimestamp);
                }
                else
                {
                    _dataDict.Add(key, value);
                    AddToTimeDict(key, value.LastActiveTimestamp);
                }
            }
        }
        public void UpdateTimeKeyDict(TKey key, long oldTimestamp, long newTimestamp)
        {
            var oldSecondKey = BuildSecondCacheKey(oldTimestamp);
            var newSecondKey = BuildSecondCacheKey(newTimestamp);

            lock (_lockObj)
            {
                //先从旧时间戳的Dict缓存中移除
                if (_timeDataDict.TryGetValue(oldSecondKey, out Dictionary<TKey, byte> oldDict))
                {
                    oldDict.Remove(key);
                }

                //再添加到新时间戳的Dict缓存下
                if (_timeDataDict.TryGetValue(newSecondKey, out Dictionary<TKey, byte> newDict))
                {
                    if (!newDict.ContainsKey(key))
                    {
                        newDict.Add(key, 1);
                    }
                }
                else
                {
                    var tempDict = new Dictionary<TKey, byte>();
                    tempDict.Add(key, 1);
                    _timeDataDict.Add(newSecondKey, tempDict);
                }
            }
        }
        public void RemoveExceedKeys()
        {
            lock (_lockObj)
            {
                var exceedCount = _dataDict.Count - _maxCacheCount;
                if (exceedCount <= 0)
                {
                    return;
                }

                var toRemoveDataCount = 0;
                var toRemovePairList = new List<KeyValuePair<long, Dictionary<TKey, byte>>>();
                var keyList = _timeDataDict.Keys.ToList();
                keyList.Sort();
                foreach (var key in keyList)
                {
                    if (_timeDataDict.TryGetValue(key, out Dictionary<TKey, byte> value))
                    {
                        toRemovePairList.Add(new KeyValuePair<long, Dictionary<TKey, byte>>(key, value));
                        toRemoveDataCount += value.Count;
                        if (toRemoveDataCount >= exceedCount)
                        {
                            break;
                        }
                    }
                }

                var removedDataCount = 0;
                foreach (var pair in toRemovePairList)
                {
                    foreach (var key in pair.Value.Keys)
                    {
                        if (_dataDict.Remove(key))
                        {
                            removedDataCount++;
                        }
                    }
                }

                _logger.InfoFormat("Removed {0}CacheKeyCount: {1}, remainingCount: {2}", _name, removedDataCount, _dataDict.Count);
            }
        }

        private void AddToTimeDict(TKey key, long timestamp)
        {
            var secondKey = BuildSecondCacheKey(timestamp);
            lock (_lockObj)
            {
                if (_timeDataDict.TryGetValue(secondKey, out Dictionary<TKey, byte> dict))
                {
                    dict.Add(key, 1);
                }
                else
                {
                    var tempDict = new Dictionary<TKey, byte>();
                    tempDict.Add(key, 1);
                    _timeDataDict.Add(secondKey, tempDict);
                }
            }
        }
        /// <summary>生产一个基于时间戳的缓存key，使用时间戳的截止到秒的时间作为缓存key，秒以下的单位的时间全部清零
        /// </summary>
        /// <param name="timestamp"></param>
        /// <returns></returns>
        private long BuildSecondCacheKey(long timestamp)
        {
            return timestamp / SecondFactor;
        }
    }

    public interface ITimeValue
    {
        long LastActiveTimestamp { get; }
    }
}
