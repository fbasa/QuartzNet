using Microsoft.Data.SqlClient;
using System.Data;

namespace QuartzNet.Service.Infrastructure;

public sealed class SqlConnectionFactory(IConfiguration cfg) : IDbConnectionFactory
{
    public async Task<IDbConnection> OpenAsync(CancellationToken ct = default)
    {
        var con = new SqlConnection(cfg.GetConnectionString("QuartzNet")!);
        await con.OpenAsync(ct);
        return con;
    }
}
