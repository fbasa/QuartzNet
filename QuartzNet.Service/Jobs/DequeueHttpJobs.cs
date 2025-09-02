using Quartz;
using System.Data;
using System.Threading.Channels;
using Microsoft.Extensions.Options;
using QuartzNet.Service.Infrastructure;

namespace QuartzNet.Service.Jobs;

[DisallowConcurrentExecution] // avoid overlapping runs of this job key
public sealed class DequeueHttpJobs(
    IHttpClientFactory http, 
    IJobQueueRepository repo,
    IOptions<AppOptions> opts,
    ILogger<DequeueHttpJobs> log) : IJob
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

        // after you claim "claimed" rows from SQL
        int workers = Math.Max(2, opts.Value.MaxPublishConcurrency);
        int capacity = 4 * workers;

        var chan = Channel.CreateBounded<JobRecord>(
                    new BoundedChannelOptions(capacity)
                    {
                        SingleReader = false,
                        SingleWriter = true,
                        FullMode = BoundedChannelFullMode.Wait
                    });

        foreach (var job in jobs)
            await chan.Writer.WriteAsync(job, ct);

        chan.Writer.Complete();

        // 2) Process each  workers job
        var tasks = ProcessTasks(workers, chan, ct);

        await Task.WhenAll(tasks);
    }

    private IEnumerable<Task> ProcessTasks(int workers, Channel<JobRecord> chan, CancellationToken ct)
    {
        var client = http.CreateClient("external-api");

        return Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            await foreach (var job in chan.Reader.ReadAllAsync(ct))
            {
                try
                {
                    using var resp = await client.PostAsJsonAsync(opts.Value.SendAndReceiveUrl, job.Payload, ct);
                    resp.EnsureSuccessStatusCode();

                    await repo.MarkJobCompletedAsync(job.JobId, ct);
                }
                catch (Exception ex)
                {
                    await repo.RequeueWithBackoffAsync(job.JobId, ex.Message, job.Attempts, ct);
                }
            }
        }));
    }
}
