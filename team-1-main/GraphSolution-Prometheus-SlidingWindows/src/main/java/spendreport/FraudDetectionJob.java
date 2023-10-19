package spendreport;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.*;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory;

import javax.annotation.Nullable;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.Summary;
import io.prometheus.client.hotspot.DefaultExports;


public class FraudDetectionJob {

	public static void main(String[] args) throws Exception {

		HTTPServer server = new HTTPServer(8080);
		DefaultExports.initialize();
		Gauge latencyGauge = Gauge.build()
				.name("latency")
				.help("The latency of the Flink job")
				.register();
		Gauge throughputGauge = Gauge.build()
				.name("throughput")
				.help("The throughput of the Flink job")
				.register();
		Gauge durationGauge = Gauge.build()
				.name("duration")
				.help("The duration of the Flink job")
				.register();
		Gauge numEventsGauge = Gauge.build()
				.name("numEvents")
				.help("The numEvents of the Flink job")
				.register();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		Counter numCheckpoints = new Counter();
		Counter numEvents = new Counter();
		Counter eventCounter ;
		long checkpointInterval = 5000;
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/seednew/Downloads/workspace/frauddetection/rocksdb_data");
		rocksDBStateBackend.setNumberOfTransferThreads(4);
		rocksDBStateBackend.setNumberOfTransferingThreads(4);
		// Flink execution environment and set the state backend

		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Set the RocksDB parallelism
		env.getConfig().setParallelism(4);
		// Add the transaction source
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions")
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Transaction>(Time.seconds(5)) {
							@Override
							public long extractTimestamp(Transaction event) {
								numEvents.inc(); // Increment the event counter for each event
								return event.getTimestamp();
							}
						}
				);
		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactions = transactions
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions").setParallelism(1);
		;


		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions = keyedTransactions
				.windowAll(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) // windowSize, change the first metric for 				different window sizes
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions")
				.setParallelism(1)
				;

		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions1 = keyedTransactions
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions1")
				.setParallelism(1)
				;


		DataStream<Tuple2<Long, Double>> merged = aggregatedTransactions.union(aggregatedTransactions1);
		merged
				.keyBy(0)
				.reduce((t1, t2) -> {
					if (t1.f1 > t2.f1) {
						return t1;
					} else {
						return t2;
					}
				})
				.print();

		// Applying the fraud detection logic on the aggregated transactions
		DataStream<Alert> alerts = aggregatedTransactions
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetector())
				.name("fraud-detector")
				.setParallelism(1)
				;

		// Sending the alerts to the alert sink
		alerts
				.addSink(new AlertSink())
				.name("send-alerts").setParallelism(1);

		// Executing the Flink job
		Summary latencySummary = Summary.build()
				.name("latency_summary")
				.help("The latency summary of the Flink job")
				.register();
		Summary throughputSummary = Summary.build()
				.name("throughput_summary")
				.help("The throughput summary of the Flink job")
				.register();
		Summary durationSummary = Summary.build()
				.name("duration_summary")
				.help("The duration summary of the Flink job")
				.register();

		Summary numEventsSummary = Summary.build()
				.name("numEvents_summary")
				.help("The numEvents summary of the Flink job")
				.register();


		// Updating the Prometheus gauges with the computed metrics


		long startTime = System.currentTimeMillis();
		JobExecutionResult result =env.execute("Fraud Detection");
		long endTime = System.currentTimeMillis();

		System.out.println("Number of checkpoints: " + result.getAccumulatorResult("numCheckpoints"));
		System.out.println("Number of events processed: " + result.getAccumulatorResult("numEvents"));
		long duration = endTime - startTime;
		int numEventss = result.getAccumulatorResult("numEvents");
		double latency = duration / (double) numEventss;
		double throughput = numEventss / ((double) duration / 1000.0);
		latencyGauge.set(latency);
		throughputGauge.set(throughput);
		numEventsGauge.set(numEventss);
		durationGauge.set(duration);
		latencySummary.observe(latency);
		throughputSummary.observe(throughput);
		durationSummary.observe(duration);
		numEventsSummary.observe(numEventss);
		// Updating the Prometheus summaries with the computed metrics
	}
	public static class Counter implements Serializable {
		private long count = 0;

		public long getCount() {
			return count;
		}

		public void inc() {
			count++;
		}
	}

	public static class TimestampAssigner extends KeyedProcessFunction<Long, Transaction, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(Transaction transaction, Context context, Collector<Tuple2<Long, Double>> out) throws Exception {
			out.collect(new Tuple2<>(transaction.getAccountId(), transaction.getAmount()));
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Double>> out) throws Exception {

		}
	}

	public static class IncrementalMeanAggregate implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> createAccumulator() {
			return new Tuple2<>(0L, 0.0);
		}

		@Override
		public Tuple2<Long, Double> add(Tuple2<Long, Double> value, Tuple2<Long, Double> accumulator) {
			return new Tuple2<>(accumulator.f0 + 1, accumulator.f1 + value.f1);
		}

		@Override
		public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
			if (accumulator.f0 == 0) {
				return new Tuple2<>(0L, 0.0);
			}
			return new Tuple2<>(accumulator.f0, accumulator.f1 / accumulator.f0);
		}

		@Override
		public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
			return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
		}
	}
	public static  class Factory{}

}
