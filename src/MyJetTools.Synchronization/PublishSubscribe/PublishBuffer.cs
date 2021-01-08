using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Prometheus;

namespace MyJetTools.Synchronization.PublishSubscribe
{
    public class PublishBuffer<T>: IDisposable
    {
        private readonly string _name;

        public static readonly Counter EventInCount = Metrics
            .CreateCounter("publish_buffer_in_count",
                "Counter of received event IN PublishBuffer.",
                new CounterConfiguration { LabelNames = new[] { "name" } });

        public static readonly Counter EventOutCount = Metrics
            .CreateCounter("publish_buffer_in_count",
                "Counter of received event OUT PublishBuffer.",
                new CounterConfiguration { LabelNames = new[] { "name" } });

        public static readonly Gauge QueueLength = Metrics
            .CreateGauge("publish_buffer_queue_length",
                "PublishBuffer query length",
                new GaugeConfiguration { LabelNames = new[] { "name" } });

        public static readonly Gauge HandlerTimeLast = Metrics
            .CreateGauge("publish_buffer_handler_time_last_ms",
                "PublishBuffer data handling delay in ms. Last value",
                new GaugeConfiguration { LabelNames = new[] { "name" } });

        public static readonly Histogram HandlerTimeLastAvg = Metrics
            .CreateHistogram("publish_buffer_handler_time_avg_ms",
                "PublishBuffer data handling delay in ms. Avg value");

        public static readonly Counter IterationWithWaitCount = Metrics
            .CreateCounter("publish_buffer_iteration_with_wait_count",
                "Counter read data iteration with wait data.",
                new CounterConfiguration { LabelNames = new[] { "name" } });

        public static readonly Counter IterationWithoutWaitCount = Metrics
            .CreateCounter("publish_buffer_iteration_without_wait_count",
                "Counter read data iteration with existing data.",
                new CounterConfiguration { LabelNames = new[] { "name" } });

        public static readonly Counter IterationSuccessCount = Metrics
            .CreateCounter("publish_buffer_read_iteration_success_count",
                "Counter success read data iteration.",
                new CounterConfiguration { LabelNames = new[] { "name" } });

        public static readonly Counter IterationFailCount = Metrics
            .CreateCounter("publish_buffer_read_iteration_success_count",
                "Counter fail read data iteration.",
                new CounterConfiguration { LabelNames = new[] { "name" } });



        private readonly ILogger _logger;
        private readonly object _gate = new object();
        private List<T> _buffer = new List<T>();

        private TaskCompletionSource<List<T>> _tcs;

        private Func<List<T>, CancellationToken, Task> _handler;

        private CancellationTokenSource _tokenAggregateSource;

        public PublishBuffer(ILoggerFactory loggerFactory, string name)
        {
            _name = name;
            _logger = loggerFactory.CreateLogger(GetType());
        }

        public void Subscribe(Func<List<T>, CancellationToken, Task> handler, CancellationToken token = default)
        {
            lock (_gate)
            {
                if (_handler != null)
                {
                    _logger.LogWarning("PublishBuffer ({name}) already has subscriber. Cannot subscribe several times", _name);
                    throw new Exception($"PublishBuffer ({_name}) already has subscriber. Cannot subscribe several times");
                }

                _handler = handler;
            }

            _tokenAggregateSource = token == default 
                ? new CancellationTokenSource() 
                : CancellationTokenSource.CreateLinkedTokenSource(token);

            Task.Run(Process, _tokenAggregateSource.Token);

            _logger.LogInformation("PublishBuffer ({name}) is Subscribed", _name);
        }

        public void UnSubscribe()
        {
            lock (_gate)
            {
                if (_tokenAggregateSource != null)
                {
                    _tokenAggregateSource.Dispose();
                    _tokenAggregateSource = null;

                    _logger.LogInformation("PublishBuffer ({name}) is UnSubscribed", _name);
                }
            }
        }

        public void Put(T item)
        {
            TaskCompletionSource<List<T>> tcs = null;
            List<T> data = null;
            lock (_gate)
            {
                if (_tokenAggregateSource == null || _tokenAggregateSource.IsCancellationRequested)
                {
                    _logger.LogWarning("PublishBuffer ({name}) are stopped and cannot register message", _name);
                    throw new Exception($"PublishBuffer ({_name}) are stopped and cannot register message");
                }

                _buffer.Add(item);

                EventInCount.Inc();
                QueueLength.Set(_buffer.Count);

                if (_tcs != null)
                {
                    tcs = _tcs;
                    _tcs = null;
                    data = _buffer;
                    _buffer = new List<T>();
                }
            }

            tcs?.TrySetResult(data);
        }

        public void PutRange(IEnumerable<T> item)
        {
            TaskCompletionSource<List<T>> tcs = null;
            List<T> data = null;
            lock (_gate)
            {
                if (_tokenAggregateSource == null || _tokenAggregateSource.IsCancellationRequested)
                {
                    _logger.LogWarning("PublishBuffer ({name}) are stopped and cannot register message", _name);
                    throw new Exception($"PublishBuffer ({_name}) are stopped and cannot register message");
                }

                var size = _buffer.Count;
                _buffer.AddRange(item);

                EventInCount.Inc(_buffer.Count - size);
                QueueLength.Set(_buffer.Count);

                if (_tcs != null)
                {
                    tcs = _tcs;
                    data = _buffer;

                    _tcs = null;
                    _buffer = new List<T>();
                    QueueLength.Set(_buffer.Count);
                }
            }

            tcs?.TrySetResult(data);
        }

        private async Task Process()
        {
            _logger.LogInformation("PublishBuffer ({name}) is started", _name);
            try
            {
                while (_tokenAggregateSource?.IsCancellationRequested == false)
                {

                    var data = await WaitBatchAsync(_tokenAggregateSource.Token);

                    EventOutCount.Inc(data.Count);

                    var timer = new Stopwatch();
                    timer.Start();
                    try
                    {
                        using (HandlerTimeLastAvg.NewTimer())
                        {
                            await _handler(data, _tokenAggregateSource.Token);
                            IterationSuccessCount.Inc();
                        }
                    }
                    catch (TaskCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        IterationFailCount.Inc();
                        _logger.LogError(e, "PublishBuffer ({name}) receive exception from handler. Data: {dataJson}",
                            _name, $"json: {JsonConvert.SerializeObject(data)}");
                    }
                    finally
                    {
                        timer.Stop();
                        HandlerTimeLast.Set(timer.ElapsedMilliseconds);
                    }
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PublishBuffer ({name}) unexpected exception", _name);
            }

            UnSubscribe();
        }

        private Task<List<T>> WaitBatchAsync(CancellationToken token)
        {
            lock (_gate)
            {
                if (_tcs != null)
                    throw new Exception("Cannot read buffer in several threads");

                if (_buffer.Any())
                {
                    var data = _buffer;
                    _buffer = new List<T>();
                    QueueLength.Set(_buffer.Count);

                    IterationWithoutWaitCount.Inc();
                    return Task.FromResult(data);
                }

                _tcs = new TaskCompletionSource<List<T>>(token);
                IterationWithWaitCount.Inc();
                return _tcs.Task;
            }
        }

        public void Dispose()
        {
            UnSubscribe();
        }
    }

    
}