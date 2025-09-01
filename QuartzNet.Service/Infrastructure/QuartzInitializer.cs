using Microsoft.Extensions.Options;
using Quartz;
using QuartzNet.Service.Jobs;

namespace QuartzNet.Service.Infrastructure;

public sealed class QuartzInitializer(ISchedulerFactory factory, IOptions<AppOptions> options) : IHostedService
{
    public async Task StartAsync(CancellationToken ct)
    {
        var scheduler = await factory.GetScheduler(ct);

        var job = JobBuilder.Create<DequeueHttpJobs>()
            .WithIdentity("dequeue-http")
            .Build();

        var trigger = TriggerBuilder.Create()
            .WithIdentity("dequeue-http-trigger")
            .StartNow()
            .WithSimpleSchedule(s => s.WithIntervalInSeconds(options.Value.IntervalInSeconds).RepeatForever())
            .Build();

        await scheduler.ScheduleJob(job, [trigger], true, ct);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
