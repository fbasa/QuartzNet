using Quartz;
using MassTransit;
using Microsoft.Extensions.Options;
using QuartzNet.Service.Contracts;
using QuartzNet.Service.Infrastructure;

namespace QuartzNet.Service.Jobs;

[DisallowConcurrentExecution] // prevents overlap of the same job key (cluster-wide)
public sealed class DequeueAndPublishHttpJob(
        IJobQueueRepository repo,
        IPublishEndpoint publish,
        IOptions<AppOptions> opts,
        ILogger<DequeueAndPublishHttpJob> log) : IJob
{
    public async Task Execute(IJobExecutionContext ctx)
    {
        var workerId = $"{Environment.MachineName}:{ctx.Scheduler.SchedulerInstanceId}";
        var ct = ctx.CancellationToken;

        // 1) Claim a batch
        var claimed = await repo.ClaimAsync(opts.Value.ClaimBatchSize, workerId, ct);
        if (claimed.Count == 0) return;
        var jobs = claimed.Where(j => j.JobType == JobType.SignalR);
        if (jobs.Count() == 0) return;
        log.LogInformation("Claimed {Count} jobs for dispatch by {Worker}", jobs.Count(), workerId);

        // 2) Publish to broker with controlled parallelism
        var parallel = Math.Max(1, opts.Value.MaxPublishConcurrency);
        var throttler = new SemaphoreSlim(parallel, parallel);
        var tasks = new List<Task>(claimed.Count);

        foreach (var job in claimed)
        {
            await throttler.WaitAsync(ct);
            tasks.Add(DispatchOne(job, throttler, ct));
        }

        await Task.WhenAll(tasks);
    }

    private async Task DispatchOne(JobRecord job, SemaphoreSlim throttler, CancellationToken ct)
    {
        try
        {
            await publish.Publish(new ProcessJob(job.JobId, job.JobType, job.Payload, job.Attempts), ct);
            await repo.MarkDispatchedAsync(job.JobId, ct);
            // NOTE: The *consumer* that performs the work should set Status=2 (done)
        }
        catch (Exception ex)
        {
            // Return to pending with backoff; keep error for diagnostics
            await repo.RequeueWithBackoffAsync(job.JobId, ex.Message, job.Attempts, ct);
        }
        finally
        {
            throttler.Release();
        }
    }
}