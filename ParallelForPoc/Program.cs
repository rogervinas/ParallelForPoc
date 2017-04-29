using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace ParallelForPoc
{
    class Program
    {
        static void Main(string[] args)
        {
            int dataCount = 100;
            int executionCount = 10;

            StrategyExecutor executor = new StrategyExecutor();

            StrategyExecutionResult resultStrategyParallelForWithLock = executor.ExecuteMany(new StrategyParallelForWithLock(), dataCount, executionCount);
            StrategyExecutionResult resultStrategyParallelForWithQueue = executor.ExecuteMany(new StrategyParallelForWithQueue(), dataCount, executionCount);

            Console.Out.WriteLine("--------------------");
            Console.Out.WriteLine("StrategyParallelForWithLock : " + resultStrategyParallelForWithLock);
            Console.Out.WriteLine("StrategyParallelForWithQueue : " + resultStrategyParallelForWithQueue);

            Console.In.ReadLine();
        }
    }

    interface IStrategy
    {
        void Execute(Process process);
    }

    class StrategyParallelForWithLock : IStrategy
    {
        public void Execute(Process process)
        {
            object mutex = new object();

            List<int> ids = process.GetIds();

            Parallel.For(0, ids.Count, i =>
            {
                Data data = process.ProduceData(ids[i]);
                lock (mutex)
                {
                    process.ConsumeData(data);
                }
            });
        }
    }

    class StrategyParallelForWithQueue : IStrategy
    {
        public void Execute(Process process)
        {
            BlockingCollection<Data> queue = new BlockingCollection<Data>();    

            List<int> ids = process.GetIds();

            Task consumer = Task.Run(() =>
            {
                int numberOfTakes = ids.Count;
                for (int i = 0; i < numberOfTakes; i++)
                {
                    Data data = queue.Take();
                    process.ConsumeData(data);
                }
            });

            Parallel.For(0, ids.Count, i =>
            {
                Data data = process.ProduceData(ids[i]);
                queue.Add(data);
            });

            consumer.Wait();
        }
    }

    class StrategyExecutor
    {
        public StrategyExecutionResult ExecuteMany(IStrategy strategy, int dataCount, int executionCount)
        {
            List<StrategyExecutionResult> results = new List<StrategyExecutionResult>();
            for (int i = 0; i < executionCount; i++)
            {
                results.Add(Execute(strategy, dataCount));
            }
            StrategyExecutionResult result = new StrategyExecutionResult();
            result.Error = results.Select(r => r.Error).Where(e => e.Length > 0).DefaultIfEmpty(string.Empty).FirstOrDefault();
            result.ElapsedMillis = (long) results.Select(r => r.ElapsedMillis).Average();
            return result;
        }

        public StrategyExecutionResult Execute(IStrategy strategy, int dataCount)
        {
            Process process = new Process(dataCount);
            Stopwatch sw = new Stopwatch();
            sw.Start();
            strategy.Execute(process);
            sw.Stop();
            return new StrategyExecutionResult()
            {
                Error = process.ValidateConsumedData(),
                ElapsedMillis = sw.ElapsedMilliseconds
            };
        }
    }

    class StrategyExecutionResult
    {
        public string Error { get; set; }
        public long ElapsedMillis { get; set; }

        public override string ToString()
        {
            return string.Format("StrategyExecutionResult ElapsedMillis: {0} {1}", ElapsedMillis, Error.Length > 0 ? "Error: " + Error : "");
        }
    }

    class Process 
    {
        private Random _random = new Random(666);

        private int _dataCount;
        private List<Data> _consumedData = new List<Data>();

        public Process(int dataCount)
        {
            this._dataCount = dataCount;
        }

        public List<int> GetIds()
        {
            return Enumerable.Range(1, _dataCount).ToList();
        }

        public Data ProduceData(int id)
        {
            TimeSpan randomSleep = RandomSleep(100, 1000);
            Data data = new Data()
            {
                Id = id,
                SomeString = string.Format("I've been sleeping for {0} millis !", randomSleep.TotalMilliseconds)
            };
            Console.Out.WriteLine(string.Format("Produced {0}", data));
            return data;
        }

        public void ConsumeData(Data data)
        {
            TimeSpan randomSleep = RandomSleep(100, 1000);
            Console.Out.WriteLine(string.Format("Consumed {0}", data));
            _consumedData.Add(data);
        }

        public string ValidateConsumedData()
        {
            List<int> consumedIds = _consumedData.Select(data => data.Id).OrderBy(e => e).ToList();
            List<int> producedIds = GetIds().OrderBy(e => e).ToList();
            if (Enumerable.SequenceEqual(consumedIds, producedIds))
            {
                return string.Empty;
            }
            else
            {
                return string.Format(
                    "ValidateConsumedData error:\n\tProduced [ {0} ]\n\tConsumed [ {1} ]",
                    string.Join(", ", producedIds), string.Join(", ", consumedIds)
                );
            }
        }

        private TimeSpan RandomSleep(int minSleepMillis, int maxSleepMillis)
        {
            int randomSleepMillis = _random.Next(minSleepMillis, maxSleepMillis);
            TimeSpan randomSleep = TimeSpan.FromMilliseconds(randomSleepMillis);
            Thread.Sleep(randomSleep);
            return randomSleep;
        }
    }

    class Data
    {
        public int Id { get; set; }
        public string SomeString { get; set; }

        public override string ToString()
        {
            return string.Format("Data Id: {0} SomeString: {1}", Id, SomeString);
        }
    }
}
