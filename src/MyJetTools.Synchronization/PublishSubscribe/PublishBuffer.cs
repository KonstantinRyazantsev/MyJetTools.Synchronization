using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace MyJetTools.Synchronization.PublishSubscribe
{
    public delegate Task SubscriberCallback<T>(List<T> items, CancellationToken cancellationToken);
    public delegate void MetricsIterationWaitCountCallback(string bufferName, bool isSyncIteration);
    public delegate void MetricsIterationCallback(string bufferName, bool isSuccess, long executionTimeMs, int batchSize);
    public delegate void MetricsQueueInOutSizeCallback(string bufferName, int countIn, int countOut, int queueSize);

    public class PublishBuffer<T>: IPublisher<T>, IDisposable
    {
        private readonly string _name;
        private readonly ILogger<PublishBuffer<T>> _logger;
        private readonly object _gate = new object();
        private List<T> _buffer = new List<T>();
        private TaskCompletionSource<List<T>> _tcs;
        private SubscriberCallback<T> _handler;
        private CancellationTokenSource _tokenAggregateSource;
        private MetricsQueueInOutSizeCallback _metricsQueueInOutSizeCallback;
        private MetricsIterationCallback _metricsIterationCallback;
        private MetricsIterationWaitCountCallback _metricsIterationWaitCountCallback;

        public PublishBuffer(ILogger<PublishBuffer<T>> logger, string name = null)
        {
            _name = name ?? $"{typeof(T).Name}";
            _logger = logger;
        }

        public static PublishBuffer<T> Create(ILogger<PublishBuffer<T>> logger, string name = null)
        {
            return new PublishBuffer<T>(logger, name);
        }

        public void Subscribe(SubscriberCallback<T> handler, CancellationToken token = default)
        {
            lock (_gate)
            {
                if (_handler != null)
                {
                    _logger.LogWarning("PublishBuffer ({PublishBufferName}) already has a subscriber. Can't subscribe several times", _name);
                    throw new InvalidOperationException($"PublishBuffer ({_name}) already has a subscriber. Can't subscribe several times");
                }

                _handler = handler;
            }

            _tokenAggregateSource = token == default 
                ? new CancellationTokenSource() 
                : CancellationTokenSource.CreateLinkedTokenSource(token);

            Task.Run(Process, _tokenAggregateSource.Token);

            _logger.LogInformation("PublishBuffer ({PublishBufferName}) subscription has been added", _name);
        }

        public void Unsubscribe()
        {
            lock (_gate)
            {
                if (_tokenAggregateSource != null)
                {
                    _tokenAggregateSource.Dispose();
                    _tokenAggregateSource = null;

                    _logger.LogInformation("PublishBuffer ({PublishBufferName}) subscription has been removed", _name);
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
                    _logger.LogWarning("PublishBuffer ({PublishBufferName}) is stopped, can't put the item", _name);
                    throw new InvalidOperationException($"PublishBuffer ({_name}) is stopped, can't put the item");
                }

                _buffer.Add(item);

                MetricsQueueInOutSize(1, 0, _buffer.Count);

                if (_tcs != null)
                {
                    tcs = _tcs;
                    _tcs = null;
                    data = _buffer;
                    _buffer = new List<T>();
                    MetricsQueueInOutSize(0, _buffer.Count, _buffer.Count);
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
                    _logger.LogWarning("PublishBuffer ({PublishBufferName}) is stopped, can't put the items", _name);
                    throw new InvalidOperationException($"PublishBuffer ({_name}) is stopped, can't put the items");
                }

                var size = _buffer.Count;
                _buffer.AddRange(item);

                MetricsQueueInOutSize(_buffer.Count - size, 0, _buffer.Count);

                if (_tcs != null)
                {
                    tcs = _tcs;
                    data = _buffer;

                    _tcs = null;
                    _buffer = new List<T>();
                    MetricsQueueInOutSize(0, _buffer.Count, _buffer.Count);
                }
            }

            tcs?.TrySetResult(data);
        }

        private async Task Process()
        {
            _logger.LogInformation("PublishBuffer ({PublishBufferName}) has been started", _name);
            try
            {
                while (_tokenAggregateSource?.IsCancellationRequested == false)
                {
                    var data = await WaitBatchAsync(_tokenAggregateSource.Token);

                    var timer = new Stopwatch();
                    timer.Start();
                    var isSuccess = true;
                    try
                    {
                        await _handler(data, _tokenAggregateSource.Token);
                    }
                    catch (TaskCanceledException)
                    {
                        throw;
                    }
                    catch (Exception e)
                    {
                        isSuccess = false;
                        _logger.LogError(e, "PublishBuffer ({PublishBufferName}) got the exception from the handler. Batch: {@PublishBufferBatch}",
                            _name, data);
                    }
                    finally
                    {
                        timer.Stop();
                        MetricsIteration(isSuccess, timer.ElapsedMilliseconds, data.Count);
                    }
                }
            }
            catch (TaskCanceledException)
            {
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "PublishBuffer ({PublishBufferName}) got the unhandled exception. Terminating.", _name);
            }

            Unsubscribe();
        }

        private Task<List<T>> WaitBatchAsync(CancellationToken token)
        {
            lock (_gate)
            {
                if (_tcs != null)
                    throw new InvalidOperationException("Can't read the buffer by several threads");

                if (_buffer.Any())
                {
                    var data = _buffer;
                    _buffer = new List<T>();
                    
                    MetricsQueueInOutSize(0, data.Count, _buffer.Count);
                    MetricsIterationWaitCount(true);

                    return Task.FromResult(data);
                }

                _tcs = new TaskCompletionSource<List<T>>(token);
                MetricsIterationWaitCount(false);
                return _tcs.Task;
            }
        }

        public void Dispose()
        {
            Unsubscribe();
        }

        /// <summary>
        /// Add callback to handle queue change.
        /// </summary>
        public PublishBuffer<T> AddMetricsQueueInOutSizeCallback(MetricsQueueInOutSizeCallback metricsQueueInOutSizeCallback)
        {
            _metricsQueueInOutSizeCallback = metricsQueueInOutSizeCallback;

            return this;
        }

        /// <summary>
        /// Add callback to handle iteration metrics.
        /// </summary>
        public PublishBuffer<T> AddMetricsIterationCallback(MetricsIterationCallback metricsIterationCallback)
        {
            _metricsIterationCallback = metricsIterationCallback;

            return this;
        }

        /// <summary>
        /// Add callback to handle notification about subscriber iteration - execute sync or async mode.
        /// </summary>
        public PublishBuffer<T> AddMetricsIterationWaitCountCallback(MetricsIterationWaitCountCallback metricsIterationWaitCountCallback)
        {
            _metricsIterationWaitCountCallback = metricsIterationWaitCountCallback;

            return this;
        }

        private void MetricsQueueInOutSize(int countIn, int countOut, int size)
        {
            _metricsQueueInOutSizeCallback?.Invoke(_name, countIn, countOut, size);
        }

        private void MetricsIteration(bool isSuccess, long executionTimeMs, int batchSize)
        {
            _metricsIterationCallback?.Invoke(_name, isSuccess, executionTimeMs, batchSize);
        }

        private void MetricsIterationWaitCount(bool isSyncIteration)
        {
            _metricsIterationWaitCountCallback?.Invoke(_name, isSyncIteration);
        }
    }
}