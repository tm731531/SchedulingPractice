using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;

namespace SubWorker.TomDemo
{
    class Program
    {
        static void Main(string[] args)
        {
            var host = new HostBuilder()
                   .ConfigureServices((context, services) =>
                   {
                    services.AddHostedService<TomSubWorkerBackgroundService>();
                   })
                   .Build();
            using (host)
            {
                host.Start();
                host.WaitForShutdown();
            }
        }
    }
}
