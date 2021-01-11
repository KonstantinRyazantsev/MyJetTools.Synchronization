using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Drawing;
using System.Dynamic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace MyJetTools.Synchronization.PublishSubscribe
{
    public class PublishBuffer<T>: IPublisher<T>, IDisposable
    {
        private readonly string _name;

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

        public static PublishBuffer<T> Create(ILoggerFactory loggerFactory, string name)
        {
            return new PublishBuffer<T>(loggerFactory, name);
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
                    _logger.LogWarning("PublishBuffer ({name}) are stopped and cannot register message", _name);
                    throw new Exception($"PublishBuffer ({_name}) are stopped and cannot register message");
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
            _logger.LogInformation("PublishBuffer ({name}) is started", _name);
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
                        _logger.LogError(e, "PublishBuffer ({name}) receive exception from handler. Data: {dataJson}",
                            _name, $"json: {JsonConvert.SerializeObject(data)}");
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
            UnSubscribe();
        }

        private Action<string, int, int, int> _metricsQueueInOutSizeCallback;
        private Action<string, bool, long, int> _metricsIterationCallback;
        private Action<string, bool> _metricsIterationWaitCountCallback;

        /// <summary>
        /// Add callback to handle queue change. Parameters:
        /// 1. int countIn
        /// 2. int countOut
        /// 3. int queueSize
        /// </summary>
        public PublishBuffer<T> AddMetricsQueueInOutSizeCallback(Action<string, int, int, int> metricsQueueInOutSizeCallback)
        {
            _metricsQueueInOutSizeCallback = metricsQueueInOutSizeCallback;

            return this;
        }

        /// <summary>
        /// Add callback to handle iteration metrics. Parameters:
        /// 1. PublishBuffer name
        /// 2. bool isSuccess - subscriber iteration finished success or with exception
        /// 3. long executionTimeMs
        /// 4. int batchSize
        /// </summary>
        public PublishBuffer<T> AddMetricsIterationCallback(Action<string, bool, long, int> metricsIterationCallback)
        {
            _metricsIterationCallback = metricsIterationCallback;

            return this;
        }

        /// <summary>
        /// Add callback to handle notification about subscriber iteration - execute sync or async mode. Parameters:
        /// 1. PublishBuffer name
        /// 2. bool isSyncIteration
        /// </summary>
        public PublishBuffer<T> AddMetricsIterationWaitCountCallback(Action<string, bool> metricsIterationWaitCountCallback)
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