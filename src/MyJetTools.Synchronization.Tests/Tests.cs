using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using MyJetTools.Synchronization.PublishSubscribe;
using NUnit.Framework;

namespace MyJetTools.Synchronization.Tests
{
    public class Tests
    {
        [SetUp]
        public void Setup()
        {
        }

        [Test]
        public void Test1()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(Handler);

            pubsub.Put(1);
            pubsub.Put(2);
            pubsub.Put(3);

            Thread.Sleep(2000);
            pubsub.Unsubscribe();
            Thread.Sleep(2000);

            Console.WriteLine("Done");
        }

        [Test]
        public void Test2()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(Handler);
            Thread.Sleep(1000);

            pubsub.Put(1);
            Thread.Sleep(1000);
            pubsub.Put(2);
            Thread.Sleep(1000);
            pubsub.Put(3);
            pubsub.Put(4);
            pubsub.Put(5);

            Thread.Sleep(1000);
            pubsub.Unsubscribe();
            Thread.Sleep(1000);

            Console.WriteLine("Done");
        }

        [Test]
        public void Test_Subscriber_Call_Sync()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerDelaySync);
            Thread.Sleep(1000);

            Console.WriteLine("put 1");
            pubsub.Put(1);
            Console.WriteLine("put 2");
            pubsub.Put(2);
            Console.WriteLine("put 3");
            pubsub.Put(3);

            Thread.Sleep(3000);
            pubsub.Unsubscribe();
            Thread.Sleep(3000);

            Console.WriteLine("Done");
        }

        [Test]
        public void Test_Subscriber_Call_ASync()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerDelayASync);
            Thread.Sleep(1000);

            Console.WriteLine("put 1");
            pubsub.Put(1);
            Console.WriteLine("put 2");
            pubsub.Put(2);
            Console.WriteLine("put 3");
            pubsub.Put(3);

            Thread.Sleep(3000);
            pubsub.Unsubscribe();
            Thread.Sleep(3000);

            Console.WriteLine("Done");
        }

        [Test]
        public void Test3()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerDelayASync);
            Thread.Sleep(1000);

            pubsub.Put(1);
            pubsub.Unsubscribe();

            Thread.Sleep(2000);
            pubsub.Unsubscribe();
            Thread.Sleep(2000);

            Console.WriteLine("Done");
        }

        [Test]
        public void Test4()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerASync);
            Thread.Sleep(1000);

            pubsub.Put(1);
            pubsub.Unsubscribe();

            try
            {
                pubsub.Put(2);
                Assert.Fail("Expect exception");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        [Test]
        public void Test5()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerASync);

            try
            {
                pubsub.Subscribe(HandlerASync);
                Assert.Fail("Expect exception");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        [Test]
        public void Test6()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(Handler);
            Thread.Sleep(1000);

            Console.WriteLine("put 1, 2");
            pubsub.PutRange(new []{1,2});
            Console.WriteLine("put 3, 4");
            pubsub.PutRange(new[] { 3,4 });



            Console.WriteLine("UnSubscribe");
            pubsub.Unsubscribe();
            
            Console.WriteLine("Done");
        }

        [Test]
        public void Test7()
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole());
            var pubsub = new PublishBuffer<int>(loggerFactory.CreateLogger<PublishBuffer<int>>(), "test");

            Console.WriteLine("Start");

            pubsub.Subscribe(HandlerASync);
            Thread.Sleep(1000);

            pubsub.PutRange(new []{1,2});
            pubsub.Unsubscribe();

            try
            {
                pubsub.PutRange(new[] { 3, 4 });
                Assert.Fail("Expect exception");
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }

        private Task Handler(List<int> arg, CancellationToken token)
        {
            Console.Write("Receive Batch: ");
            foreach (int i in arg)
            {
                Console.Write($"{i}, ");
            }
            Console.WriteLine();

            return Task.CompletedTask;
        }

        private Task HandlerDelaySync(List<int> arg, CancellationToken token)
        {
            Console.Write("Receive Batch: ");
            foreach (int i in arg)
            {
                Console.Write($"{i}, ");
            }
            Console.WriteLine();
            Thread.Sleep(2000);
            Console.WriteLine("Handler finished");

            return Task.CompletedTask;
        }

        private async Task HandlerDelayASync(List<int> arg, CancellationToken token)
        {
            Console.Write("Receive Batch: ");
            foreach (int i in arg)
            {
                Console.Write($"{i}, ");
            }
            Console.WriteLine();
            await Task.Delay(2000, token);
            Console.WriteLine("Handler finished");
        }

        private async Task HandlerASync(List<int> arg, CancellationToken token)
        {
            Console.Write("Receive Batch: ");
            foreach (int i in arg)
            {
                Console.Write($"{i}, ");
            }
            Console.WriteLine();
            await Task.Delay(1);
            Console.WriteLine("Handler finished");
        }
    }
}