using Microsoft.Extensions.Hosting;
using System.Threading;
using System.Threading.Tasks;

namespace SubWorker.TomDemo
{
    class TomSubWorkerBackgroundService : BackgroundService
    {
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            throw new System.NotImplementedException();
        }
    }
}