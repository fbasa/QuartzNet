using MassTransit;

namespace QuartzNet.Service.Messaging;

public static class BusConfig
{
    public static IServiceCollection AddAppBus(this IServiceCollection services, IConfiguration cfg)
    {
        var host = cfg["RabbitMq:Host"] ?? "localhost";
        var user = cfg["RabbitMq:User"] ?? "guest";
        var pass = cfg["RabbitMq:Pass"] ?? "guest";

        services.AddMassTransit(x =>
        {
            // add your consumers here, or keep only publishing in this process
            x.AddConsumer<HttpJobConsumer>(c => c.ConcurrentMessageLimit = 16);

            x.UsingRabbitMq((context, busCfg) =>
            {
                busCfg.Host(host, "/", h =>
                {
                    h.Username(user);
                    h.Password(pass);
                });

                busCfg.ConfigureEndpoints(context, new KebabCaseEndpointNameFormatter("app", false));
            });
        });

        return services;
    }
}
