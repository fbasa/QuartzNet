using System.Data;

namespace QuartzNet.Service.Infrastructure;

public interface IDbConnectionFactory
{
    Task<IDbConnection> OpenAsync(CancellationToken ct = default);
}
