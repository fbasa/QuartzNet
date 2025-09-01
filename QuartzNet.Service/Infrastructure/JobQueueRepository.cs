using Dapper;

namespace QuartzNet.Service.Infrastructure;

public sealed class JobQueueRepository(IDbConnectionFactory factory) : IJobQueueRepository
{
    public async Task<IReadOnlyList<JobRecord>> ClaimAsync(int batch, string workerId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);

        const string sql = @"
                SET XACT_ABORT ON;
                BEGIN TRAN;
                DECLARE @claimed TABLE(JobId bigint, JobType nvarchar(64), Payload nvarchar(max), Attempts int);
                WITH cte(JobId,JobType,Payload,Status,Attempts,LockedAt,LockedBy) AS (
                  SELECT TOP (25) JobId, JobType, Payload,Status,Attempts,LockedAt,LockedBy
                  FROM [dbo].JobQueue WITH (READPAST, UPDLOCK, ROWLOCK)
                  WHERE Status = 0 AND AvailableAt <= SYSUTCDATETIME()
                  ORDER BY JobId
                )
                UPDATE cte
                   SET Status=1, Attempts=Attempts+1, LockedAt=SYSUTCDATETIME(), LockedBy=@worker
                OUTPUT inserted.JobId, inserted.JobType, inserted.Payload, inserted.Attempts INTO @claimed;
                SELECT JobId, JobType, Payload, Attempts FROM @claimed;
                COMMIT;
";
        var rows = await con.QueryAsync<JobRecord>(new CommandDefinition(sql, new { batch, worker = workerId }, cancellationToken: ct));
        return rows.AsList();
    }

    public async Task RequeueWithBackoffAsync(long jobId, string error, int attempts, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);

        const string sql = @"
UPDATE dbo.JobQueue
   SET Status = 0,
       AvailableAt = DATEADD(SECOND,
                             CASE WHEN @attempts > 10 THEN 300 ELSE CONVERT(int, POWER(2, @attempts)) END,
                             SYSUTCDATETIME()),
       LastError = LEFT(@error, 4000),
       LockedBy = NULL,
       LockedAt = NULL
 WHERE JobId = @jobId;
";
        await con.ExecuteAsync(new CommandDefinition(sql, new { jobId, error, attempts }, cancellationToken: ct));
    }

    public async Task MarkDispatchedAsync(long jobId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        const string sql = @"UPDATE dbo.JobQueue SET DispatchedAt = SYSUTCDATETIME() WHERE JobId = @jobId;";
        await con.ExecuteAsync(new CommandDefinition(sql, new { jobId }, cancellationToken: ct));
    }

    public async Task MarkJobCompletedAsync(long jobId, CancellationToken ct)
    {
        using var con = await factory.OpenAsync(ct);
        const string sql = @"UPDATE dbo.JobQueue SET Status=2, LockedAt=NULL, LockedBy=NULL, LastError=NULL WHERE JobId = @jobId;";
        await con.ExecuteAsync(new CommandDefinition(sql, new { jobId }, cancellationToken: ct));
    }
}
