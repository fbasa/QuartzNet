using Quartz;
using QuartzNet.Service.Jobs;

namespace QuartzNet.Service.Infrastructure;

public sealed class QuartzInitializer : IHostedService
{
    private readonly ISchedulerFactory _factory;
    public QuartzInitializer(ISchedulerFactory factory) => _factory = factory;

    public async Task StartAsync(CancellationToken ct)
    {
        var scheduler = await _factory.GetScheduler(ct);

        var job = JobBuilder.Create<DequeueHttpJobs>()
            .WithIdentity("dequeue-http")
            .Build();

        var trigger = TriggerBuilder.Create()
            .WithIdentity("dequeue-http-trigger")
            .StartNow()
            .WithSimpleSchedule(s => s.WithIntervalInSeconds(10).RepeatForever())
            .Build();

        await scheduler.ScheduleJob(job, [trigger], true, ct);
    }

    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
