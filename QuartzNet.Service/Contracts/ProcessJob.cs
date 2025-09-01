namespace QuartzNet.Service.Contracts;

public sealed record ProcessJob(
    long JobId,
    string JobType,
    string Payload,        // consider validating/typing this per JobType
    int Attempts);


