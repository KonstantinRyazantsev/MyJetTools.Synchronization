using System;
using MyJetTools.Synchronization.PublishSubscribe;
using Prometheus;

namespace MyJetTools.Synchronization.Prometheus
{
    public static class PrometheusMetrics
    {
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

        public static readonly Gauge HandlerTimePerEventLast = Metrics
            .CreateGauge("publish_buffer_handler_time_per_event_last_ms",
                "PublishBuffer data handling delay in ms. Last value average per event",
                new GaugeConfiguration { LabelNames = new[] { "name" } });


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

        public static void MetricsQueueInOutSize(string name, int countIn, int countOut, int size)
        {
            if (countIn > 0)
                EventInCount.Inc(countIn);

            if (countOut > 0)
                EventOutCount.Inc(countOut);

            QueueLength.Set(size);
        }

        public static void MetricsIteration(string name, bool isSuccess, long executionTimeMs, int batchSize)
        {
            HandlerTimeLast.Set(executionTimeMs);

            var delay = Math.Round((double)executionTimeMs / batchSize, 3);
            HandlerTimePerEventLast.Set(delay);

            HandlerTimeLastAvg.Observe(executionTimeMs);

            if (isSuccess)
            {
                IterationSuccessCount.Inc();
            }
            else
            {
                IterationFailCount.Inc();
            }
        }

        public static void MetricsIterationWaitCount(string name, bool isSyncIteration)
        {
            if (isSyncIteration)
            {
                IterationWithoutWaitCount.Inc();
            }
            else
            {
                IterationWithWaitCount.Inc();
            }
        }


        public static PublishBuffer<T> AddPrometheus<T>(this PublishBuffer<T> instance)
        {
            instance
                .AddMetricsIterationCallback(MetricsIteration)
                .AddMetricsQueueInOutSizeCallback(MetricsQueueInOutSize)
                .AddMetricsIterationWaitCountCallback(MetricsIterationWaitCount);

            return instance;
        }
    }
}
