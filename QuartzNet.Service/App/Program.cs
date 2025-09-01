using MassTransit;
using Quartz;
using QuartzNet.Service.Infrastructure;
using QuartzNet.Service.Jobs;
using QuartzNet.Service.Messaging;

var builder = WebApplication.CreateBuilder(args);

builder.Services.Configure<AppOptions>(builder.Configuration.GetSection(AppOptions.SectionName));

// Dapper connection factory + repo
builder.Services.AddSingleton<IDbConnectionFactory, SqlConnectionFactory>();
builder.Services.AddSingleton<IJobQueueRepository, JobQueueRepository>();

// Http client for any consumers
builder.Services.AddHttpClient("external-api")
    .ConfigureHttpClient(c => c.Timeout = TimeSpan.FromSeconds(10));

var opts = builder.Configuration.GetSection(AppOptions.SectionName).Get<AppOptions>()!;

// Quartz (with persistent store & clustering optional)
builder.Services.AddQuartz(q =>
{
    q.UsePersistentStore(s =>
    {
        s.UseSqlServer(x => x.ConnectionString = builder.Configuration.GetConnectionString("QuartzNet")!);
        s.UseProperties = true;
        s.UseClustering();
        s.UseNewtonsoftJsonSerializer();
    });

    if (opts.UseRabbitMQ)
    {
        var jobKey = new JobKey("dequeue-and-publish", opts.WorkerGroup);
        q.AddJob<DequeueAndPublishHttpJob>(o => o.WithIdentity(jobKey).StoreDurably());
        q.AddTrigger(t => t
            .ForJob(jobKey)
            .WithIdentity("dequeue-and-publish-trigger", opts.WorkerGroup)
            .StartNow()
            .WithSimpleSchedule(s => s.WithIntervalInSeconds(opts.IntervalInSeconds).RepeatForever()));
    }
});

builder.Services.AddQuartzHostedService(o => o.WaitForJobsToComplete = true);

if (opts.UseRabbitMQ)
{
    // MassTransit + RabbitMQ
    builder.Services.AddAppBus(builder.Configuration);

    // Quartz job + consumer DI
    builder.Services.AddTransient<DequeueAndPublishHttpJob>();
}
else
{
    builder.Services.AddHostedService<QuartzInitializer>();

}

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var factory = scope.ServiceProvider.GetRequiredService<ISchedulerFactory>();
    var scheduler = await factory.GetScheduler();

    if (opts.UseRabbitMQ)
    {
        // If using RabbitMQ, pause the HTTP dequeue job
        await scheduler.PauseJob(new JobKey("dequeue-http"));
    }
    else
    {
        // If NOT using RabbitMQ, pause the dequeue-and-publish job
        await scheduler.PauseJob(new JobKey("dequeue-and-publish", opts.WorkerGroup));
    }
}

app.MapGet("/", () => "Scheduler up");

app.Run();
