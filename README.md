# MyJetTools.Synchronization

## PublishBuffer - buffer with many publishers and single handler

PublishBuffer implement pattern with many async publisher in queue and single handler. PublishBuffer implements a pattern with many queued asynchronous publishers and one handler. The handler processes messages in batches

using example:

```csharp

static void Main(string[] args)
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
    var pubsub = PublishBuffer<int>.Create(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

    // activate subscriber
    pubsub.Subscribe(Handler);

    // simulate publisher
    while(true)
    {
        pubsub.Put(1);
        pubsub.Put(2);
        pubsub.Put(3);
        pubsub.Put(4);
    }
    
    // clear
    pubsub.Dispose();
}

private static async Task Handler(List<int> items, CancellationToken cancellationToken)
{
    Console.Write($"Receive Batch ({cancellationToken.IsCancellationRequested}): ");
    foreach (int i in items)
    {
        Console.Write($"{i}, ");
    }
    Console.WriteLine();
    await Task.Delay(1000, cancellationToken);    
}
```

# MyJetTools.Synchronization.Prometheus

## PublishBuffer

To setup PublishBuffer with Prometheus metrics:

1. add nuget library `MyJetTools.Synchronization.Prometheus`
2. setup metrics
```
var pubsub = PublishBuffer<int>.Create(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test").AddPrometheus();
```
