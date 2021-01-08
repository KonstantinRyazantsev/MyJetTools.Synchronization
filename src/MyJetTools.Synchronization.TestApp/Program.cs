using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetTools.Synchronization.PublishSubscribe;

namespace MyJetTools.Synchronization.TestApp
{
    class Program
    {
        static void Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory, "test");

            var token = new CancellationTokenSource();
            Console.WriteLine("Start");

            pubsub.Subscribe(Handler, token.Token);
            Thread.Sleep(2000);
            pubsub.Put(1);

            token.Cancel();
            Thread.Sleep(2000);

            Console.WriteLine("Press enter ...");
            Console.ReadLine();
            Console.WriteLine("Done");

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
            await Task.Delay(10000, token);
            Console.WriteLine("HandlerFinished");
        }
    }
}
