using System;
using System.Collections;
using System.Collections.Generic;

namespace EStore
{
    /// <summary>
    /// 一个布隆过滤器是一个空间有效的概率数据结构
    /// 用于测试一个元素是否是一个集合的成员。误检率是可能的，但漏检率是不存在的。元素可以被添加到集合，但不能从集合删除。
    /// </summary>
    /// <typeparam name="Type">泛型数据类型</typeparam>
    public class BloomFilter<T>
    {
        private Random _random;
        private int _bitSize, _numberOfHashes, _setSize;
        private BitArray _bitArray;

        //static void Main()
        //{
        //    BloomFilter<string> bf = new BloomFilter<string>(20, 3);

        //    bf.Add("testing");
        //    bf.Add("nottesting");
        //    bf.Add("testingagain");

        //    Console.WriteLine(bf.Contains("badstring")); // False
        //    Console.WriteLine(bf.Contains("testing")); // True

        //    List<string> testItems = new List<string>() { "badstring", "testing", "test" };

        //    Console.WriteLine(bf.ContainsAll(testItems)); // False
        //    Console.WriteLine(bf.ContainsAny(testItems)); // True

        //    //误检率: 0.040894188143892
        //    Console.WriteLine("False Positive Probability: " + bf.FalsePositiveProbability());

        //    Console.ReadKey();
        //}

        #region Constructors
        /// <summary>
        /// 初始化bloom滤波器并设置hash散列的最佳数目
        /// </summary>
        /// <param name="bitSize">布隆过滤器的大小(m)</param>
        /// <param name="setSize">集合的大小 (n)</param>
        public BloomFilter(int bitSize, int setSize)
        {
            _bitSize = bitSize;
            _bitArray = new BitArray(bitSize);
            _setSize = setSize;
            _numberOfHashes = OptimalNumberOfHashes(_bitSize, _setSize);
        }

        //<param name="numberOfHashes">hash散列函数的数量(k)</param>
        public BloomFilter(int bitSize, int setSize, int numberOfHashes)
        {
            _bitSize = bitSize;
            _bitArray = new BitArray(bitSize);
            _setSize = setSize;
            _numberOfHashes = numberOfHashes;
        }
        #endregion

        #region 属性
        public int NumberOfHashes
        {
            set
            {
                _numberOfHashes = value;
            }
            get
            {
                return _numberOfHashes;
            }
        }
        public int SetSize
        {
            set
            {
                _setSize = value;
            }
            get
            {
                return _setSize;
            }
        }
        public int BitSize
        {
            set
            {
                _bitSize = value;
            }
            get
            {
                return _bitSize;
            }
        }
        #endregion

        #region 公共方法
        public void Add(T item)
        {
            _random = new Random(Hash(item));

            for (int i = 0; i < _numberOfHashes; i++)
                _bitArray[_random.Next(_bitSize)] = true;
        }
        public bool Contains(T item)
        {
            _random = new Random(Hash(item));

            for (int i = 0; i < _numberOfHashes; i++)
            {
                if (!_bitArray[_random.Next(_bitSize)])
                    return false;
            }
            return true;
        }

        //检查列表中的任何项是否可能是在集合。
        //如果布隆过滤器包含列表中的任何一项，返回真
        public bool ContainsAny(List<T> items)
        {
            foreach (T item in items)
            {
                if (Contains(item))
                    return true;
            }
            return false;
        }

        //检查列表中的所有项目是否都在集合。
        public bool ContainsAll(List<T> items)
        {
            foreach (T item in items)
            {
                if (!Contains(item))
                    return false;
            }

            return true;
        }

        /// <summary>
        /// 计算遇到误检率的概率。
        /// </summary>
        /// <returns>Probability of a false positive</returns>
        public double FalsePositiveProbability()
        {
            return Math.Pow((1 - Math.Exp(-_numberOfHashes * _setSize / (double)_bitSize)), _numberOfHashes);
        }
        #endregion

        #region 私有方法
        private int Hash(T item)
        {
            return item.GetHashCode();
        }

        //计算基于布隆过滤器散列的最佳数量
        private int OptimalNumberOfHashes(int bitSize, int setSize)
        {
            return (int)Math.Ceiling((bitSize / setSize) * Math.Log(2.0));
        }
        #endregion
    }
}