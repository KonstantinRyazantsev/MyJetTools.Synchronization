# MyJetTools.Synchronization

using example:

```csharp

static void Main(string[] args)
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
    var pubsub = new PublishBuffer<int>(loggerFactory, "test");

    // activate subscriber
    pubsub.Subscribe(Handler);

    // simulate publisher
    while(true)
    pubsub.Put(1);
    pubsub.Put(2);
    pubsub.Put(3);
    pubsub.Put(4);
    
    // clear
    pubsub.Dispose();
}

private static async Task Handler(List<int> arg, CancellationToken token)
{
    Console.Write($"Receive Batch ({token.IsCancellationRequested}): ");
    foreach (int i in arg)
    {
        Console.Write($"{i}, ");
    }
    Console.WriteLine();
    await Task.Delay(1000, token);    
}
```