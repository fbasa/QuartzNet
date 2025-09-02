using Dapper;

namespace QuartzNet.Service.Infrastructure;

public sealed class JobQueueRepository(IDbConnectionFactory factory) : IJobQueueRepository
{
    public async Task<IReadOnlyList<JobRecord>> ClaimAsync(int batch, string workerId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        var rows = await con.QueryAsync<JobRecord>(new CommandDefinition(SqlTemplates.ClaimJobs, new { batch, worker = workerId }, cancellationToken: ct));
        return rows.AsList();
    }

    public async Task RequeueWithBackoffAsync(long jobId, string error, int attempts, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        await con.ExecuteAsync(new CommandDefinition(SqlTemplates.RequeWithBackoff, new { jobId, error, attempts }, cancellationToken: ct));
    }

    public async Task MarkDispatchedAsync(long jobId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        await con.ExecuteAsync(new CommandDefinition(SqlTemplates.MarkDispatched, new { jobId }, cancellationToken: ct));
    }

    public async Task MarkJobCompletedAsync(long jobId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        await con.ExecuteAsync(new CommandDefinition(SqlTemplates.MarkJobCompleted, new { jobId }, cancellationToken: ct));
    }
}
