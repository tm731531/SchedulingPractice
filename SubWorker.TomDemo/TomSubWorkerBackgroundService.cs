using Microsoft.Extensions.Hosting;
using SchedulingPractice.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SubWorker.TomDemo
{
    /*
     目標
任務的需求 (必須全部都滿足):

所有 job 都必須在指定時間範圍內被啟動 (預約時間 + 可接受的延遲範圍)
每個 job 都只能被執行一次, 不能有處理到一半掛掉的狀況
必須支援分散式執行 (多組 worker, 能分散負載, 能互相備援, 支援動態 scaling)
         */
    class TomSubWorkerBackgroundService : BackgroundService
    {
        private BlockingCollection<JobInfo> _queue = new BlockingCollection<JobInfo>();


        protected async override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await Task.Delay(1);
            Thread[] threads = CollectionWorker();
            await InitJobs(stoppingToken);
            foreach (var t in threads)
            {
                t.Join();
            }
        }

        private async Task InitJobs(CancellationToken stoppingToken)
        {
            using (JobsRepo repo = new JobsRepo())
            {
                while (true)
                {
                    /// 取消的時候停止塞QUEUE  如果拿掉了 可能會多打一次無效的DB
                    if (stoppingToken.IsCancellationRequested)
                    {
                        StopAddQueue();
                        break;
                    }

                    Console.WriteLine($"[T: {Thread.CurrentThread.ManagedThreadId}] fetch available jobs from repository...");
                    foreach (var job in repo.GetReadyJobs(JobSettings.MinPrepareTime))
                    {
                        /// 取消的時候停止塞QUEUE
                        if (stoppingToken.IsCancellationRequested)
                        {
                            StopAddQueue();
                            break;
                        }

                        int predict_time = 1000;//一秒
                        if (job.RunAt - DateTime.Now > TimeSpan.FromMilliseconds(predict_time))
                        {
                            try
                            {
                                await Task.Delay(job.RunAt - DateTime.Now - TimeSpan.FromMilliseconds(predict_time), stoppingToken);
                            }
                            catch
                            {
                                StopAddQueue();
                                break;
                            }
                        }

                        if (repo.GetJob(job.Id).State != 0) continue;
                        //思考等待還是重新查表好
                        if (DateTime.Now < job.RunAt) continue;//重新拉表
                        if (repo.AcquireJobLock(job.Id) == false) continue;

                        AddQueue(job);
                    }
                }


            }
        }

        private void AddQueue(JobInfo job)
        {
            this._queue.Add(job);
        }

        private void StopAddQueue()
        {
            this._queue.CompleteAdding();
        }
        private Thread[] CollectionWorker()
        {
            int processThreadsTotal = 5;
            Thread[] threads = new Thread[processThreadsTotal];
            for (int i = 0; i < processThreadsTotal; i++)
            {
                threads[i] = new Thread(this.DoJobInQueue);
            }

            foreach (var t in threads)
            {
                t.Start();
            }
            return threads;
        }

        private void DoJobInQueue()
        {
            using (JobsRepo repo = new JobsRepo())
            {
                foreach (var job in this._queue.GetConsumingEnumerable())
                {
                    repo.ProcessLockedJob(job.Id);
                    Console.WriteLine($"[Thread: {Thread.CurrentThread.ManagedThreadId}] ID ({job.Id}) " +
                        $"delay {(DateTime.Now - job.RunAt).TotalMilliseconds} ");
                }
            }
        }
    }
}