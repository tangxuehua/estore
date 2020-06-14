using ECommon.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace EStore
{
    public class CacheDict<TKey, TValue> where TValue : ITimeValue
    {
        private readonly long SecondFactor = 10000000L;
        private readonly ConcurrentDictionary<TKey, TValue> _dataDict = new ConcurrentDictionary<TKey, TValue>();
        private readonly SortedDictionary<long, ConcurrentDictionary<TKey, byte>> _timeSortedDataDict = new SortedDictionary<long, ConcurrentDictionary<TKey, byte>>();
        private readonly string _name;
        private readonly ILogger _logger;
        private readonly int _maxCacheCount;

        public CacheDict(string name, ILogger logger, int maxCacheCount)
        {
            _name = name;
            _logger = logger;
            _maxCacheCount = maxCacheCount;
        }

        public bool Exist(TKey key)
        {
            return _dataDict.ContainsKey(key);
        }
        public TValue Get(TKey key)
        {
            if (_dataDict.TryGetValue(key, out TValue value))
            {
                return value;
            }
            return default(TValue);
        }
        public TValue Add(TKey key, TValue value)
        {
            return _dataDict.GetOrAdd(key, value);
        }
        public void UpdateTimeDict(TKey key, TValue value)
        {
            var oldSecondKey = BuildSecondCacheKey(value.LastActiveTimestamp);
            var newSecondKey = BuildSecondCacheKey(DateTime.Now.Ticks);

            //先从旧时间戳的Dict缓存中移除
            if (_timeSortedDataDict.TryGetValue(oldSecondKey, out ConcurrentDictionary<TKey, byte> dict1))
            {
                dict1.TryRemove(key, out byte removed);
            }

            //再添加到新时间戳的Dict缓存下
            if (_timeSortedDataDict.TryGetValue(newSecondKey, out ConcurrentDictionary<TKey, byte> dict2))
            {
                dict2.TryAdd(key, 1);
            }
            else
            {
                var tempDict = new ConcurrentDictionary<TKey, byte>();
                tempDict.TryAdd(key, 1);
                _timeSortedDataDict.Add(newSecondKey, tempDict);
            }
        }
        public void AddToTimeDict(TKey key, TValue value)
        {
            var secondKey = BuildSecondCacheKey(value.LastActiveTimestamp);

            if (_timeSortedDataDict.TryGetValue(secondKey, out ConcurrentDictionary<TKey, byte> dict))
            {
                dict.TryAdd(key, 1);
            }
            else
            {
                var tempDict = new ConcurrentDictionary<TKey, byte>();
                tempDict.TryAdd(key, 1);
                _timeSortedDataDict.Add(secondKey, tempDict);
            }
        }
        public void RemoveExceedKeys()
        {
            var exceedCount = _dataDict.Count - _maxCacheCount;
            if (exceedCount <= 0)
            {
                return;
            }

            var toRemoveDataCount = 0;
            var toRemovePairList = new List<KeyValuePair<long, ConcurrentDictionary<TKey, byte>>>();
            foreach (var entry in _timeSortedDataDict)
            {
                toRemovePairList.Add(entry);
                toRemoveDataCount += entry.Value.Count;
                if (toRemoveDataCount >= exceedCount)
                {
                    break;
                }
            }

            var removedDataCount = 0;
            foreach (var pair in toRemovePairList)
            {
                _timeSortedDataDict.Remove(pair.Key);
                foreach (var dataKey in pair.Value.Keys)
                {
                    if (_dataDict.TryRemove(dataKey, out TValue removed))
                    {
                        removedDataCount++;
                    }
                }
            }

            _logger.InfoFormat("Removed {0}CacheCount: {1}, remainingCount: {2}", _name, removedDataCount, _dataDict.Count);
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
