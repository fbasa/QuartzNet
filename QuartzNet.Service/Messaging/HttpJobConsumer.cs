using MassTransit;
using Microsoft.Extensions.Options;
using QuartzNet.Service.Contracts;
using QuartzNet.Service.Infrastructure;

namespace QuartzNet.Service.Messaging;

public sealed class HttpJobConsumer(
    IHttpClientFactory http, 
    IJobQueueRepository repo,
    IOptions<AppOptions> opts) : IConsumer<ProcessJob>
{
    public async Task Consume(ConsumeContext<ProcessJob> context)
    {
        var msg = context.Message;

        // here you would switch on msg.JobType and route to proper handler
        var client = http.CreateClient("external-api");

        try
        {
            using var resp = await client.PostAsJsonAsync(opts.Value.SendAndReceiveUrl, context.CancellationToken);
            resp.EnsureSuccessStatusCode();

            // mark done (the worker, not the scheduler, owns completion)
            await repo.MarkJobCompletedAsync(msg.JobId, context.CancellationToken);
        }
        catch (Exception ex)
        {
            // Return to pending with backoff; keep error for diagnostics
            await repo.RequeueWithBackoffAsync(msg.JobId, ex.Message, msg.Attempts, context.CancellationToken);
        }
    }
}