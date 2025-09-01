namespace QuartzNet.Service.Infrastructure;

public sealed record JobRecord(long JobId, string JobType, string Payload, int Attempts);
