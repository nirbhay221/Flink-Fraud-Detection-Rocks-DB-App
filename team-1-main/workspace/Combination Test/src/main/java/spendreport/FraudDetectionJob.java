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
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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


public class FraudDetectionJob {
	public static SinkFunction out = null;

	public static void main(String[] args) throws Exception {
//		System.out.println("Test");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		Counter numCheckpoints = new Counter();
		Counter numEvents = new Counter();
		long checkpointInterval = 5000;
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/user/Desktop/Stream-CS551/project/team-1/SlidingWindowMetrics/rocksdb_data");
		rocksDBStateBackend.setNumberOfTransferThreads(4);
		rocksDBStateBackend.setNumberOfTransferingThreads(4);
		// Flink execution environment and set the state backend

		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Set the RocksDB parallelism
		env.getConfig().setParallelism(1);

		//
		//
		// 	Sliding Window
		//
		//

		// Add the transaction source
		DataStream<Transaction> transactionsSliding = env
				.addSource(new TransactionSource())
				.name("transactions of Sliding Windows with Slicing")
				.assignTimestampsAndWatermarks(
						new BoundedOutOfOrdernessTimestampExtractor<Transaction>(Time.seconds(5)) {
							@Override
							public long extractTimestamp(Transaction event) {
								numEvents.inc(); // Increment the event counter for each event
								return event.getTimestamp();
							}
						}
				);
		transactionsSliding.print("Sliding Window Transaction: ");
//		printOrTest(transactions);
//		out.collect();

		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactionsSliding = transactionsSliding
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions of Sliding Windows with Slicing").setParallelism(1);
		;

		keyedTransactionsSliding.print("Sliding Window keyed Transaction: ");

		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactionsSliding = keyedTransactionsSliding
				.windowAll(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) // windowSize, change the first metric for different window sizes
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions of Sliding Windows with Slicing")
				.setParallelism(1)
				;

		aggregatedTransactionsSliding.print("Sliding Window aggregated Transaction: ");

//		aggregatedTransactions.print();
		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
//		DataStream<Tuple2<Long, Double>> aggregatedTransactions1 = keyedTransactions1
//				.keyBy(0)
//				.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) // windowSize, change the first metric for different window sizes
//				.aggregate(new IncrementalMeanAggregate())
//				.name("aggregated-transactions")
//				.setParallelism(1)
//				;

//		aggregatedTransactions1.print();


		// Applying the fraud detection logic on the aggregated transactions
		DataStream<Alert> alertsSliding = aggregatedTransactionsSliding
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetectorSliding())
				.name("fraud-detector of Sliding Windows with Slicing")
				.setParallelism(1)
				;

		alertsSliding.print("Sliding Window Alert: ");

//		aggregatedTransactions.print();
//		printOrTest(aggregatedTransactions);

		// Sending the alerts to the alert sink
		alertsSliding
				.addSink(new AlertSink())
				.name("send-alerts of Sliding Windows with Slicing").setParallelism(1);

//		alerts.print();
		// Executing the Flink job


		//
		//
		//	Tumbling Window
		//
		//

		DataStream<Transaction> transactionsTumbling = env
				.addSource(new TransactionSource())
				.name("transactions of Tumbling Windows with Record buffer")
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
						.withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

		transactionsTumbling.print("Tumbling Window Transaction: ");

		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactionsTumbling = transactionsTumbling
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions of Tumbling Windows with Record buffer");

		keyedTransactionsTumbling.print("Tumbling Window keyed Transaction: ");

		// Use a tumbling window of 5 minutes to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactionsTumbling = keyedTransactionsTumbling
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.sum(1);

		aggregatedTransactionsTumbling.print("Tumbling Window aggregated Transaction: ");

		// Apply the fraud detection logic on the aggregated transactions
		DataStream<Alert> alertsTumbling = aggregatedTransactionsTumbling
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetectorTumbling())
				.name("fraud-detector of Tumbling Windows with Record buffer");

		alertsTumbling.print("Tumbling Window Alert: ");

		// Send the alerts to the alert sink
		alertsTumbling
				.addSink(new AlertSink())
				.name("send-alerts of Tumbling Windows with Record buffer");


		//
		//
		//	Inorder Sliding
		//
		//

		// Add the transaction source
		DataStream<Transaction> transactionsInorderSliding = env
				.addSource(new TransactionSource())
				.name("transactions of Inorder Sliding")
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Transaction>() {
							@Override
							public long extractAscendingTimestamp(Transaction event) {
								return event.getTimestamp();
							}
						}
				);

		transactionsInorderSliding.print("Slding Window Inorder Transaction: ");

		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactionsInorderSliding = transactionsInorderSliding
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions of Inorder Sliding").setParallelism(1);
		;

		keyedTransactionsInorderSliding.print("Slding Window Inorder keyed Transaction: ");

		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactionsInorderSliding = keyedTransactionsInorderSliding
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions of Inorder Sliding")
				.setParallelism(1)
				;

		aggregatedTransactionsInorderSliding.print("Slding Window Inorder aggregated Transaction: ");

		// Applying the fraud detection logic on the aggregated transactions
		DataStream<Alert> alertsInorderSliding = aggregatedTransactionsInorderSliding
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetectorSliding())
				.name("fraud-detector of Inorder Sliding")
				.setParallelism(1)
				;

		alertsInorderSliding.print("Slding Window Inorder Alert: ");

		// Sending the alerts to the alert sink
		alertsInorderSliding
				.addSink(new AlertSink())
				.name("send-alerts of Inorder Sliding").setParallelism(1);


		//
		//
		//	Inorder Tumbling
		//
		//

		DataStream<Transaction> transactionsInorderTumbling = env
				.addSource(new TransactionSource())
				.name("transactions of Inorder Tumbling")
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Transaction>() {
							@Override
							public long extractAscendingTimestamp(Transaction event) {
								return event.getTimestamp();
							}
						}
				);

		transactionsInorderTumbling.print("Tumbling Window Inorder Transaction: ");

		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactionsInorderTumbling = transactionsInorderTumbling
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions of Inorder Tumbling");

		keyedTransactionsInorderTumbling.print("Tumbling Window Inorder keyed Transaction: ");

		// Use a tumbling window of 5 minutes to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactionsInorderTumbling = keyedTransactionsInorderTumbling
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.sum(1);

		aggregatedTransactionsInorderTumbling.print("Tumbling Window Inorder aggregated Transaction: ");

		// Apply the fraud detection logic on the aggregated transactions
		DataStream<Alert> alertsInorderTumbling = aggregatedTransactionsInorderTumbling
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetectorTumbling())
				.name("fraud-detector of Inorder Tumbling");

		alertsInorderTumbling.print("Tumbling Window Inorder Alert: ");

		// Send the alerts to the alert sink
		alertsInorderTumbling
				.addSink(new AlertSink())
				.name("send-alerts of Inorder Tumbling");



		long startTime = System.currentTimeMillis();
		JobExecutionResult result =env.execute("Fraud Detection with Sliding and Tumbling");
		long endTime = System.currentTimeMillis();
		System.out.println("Number of checkpoints: " + result.getAccumulatorResult("numCheckpoints"));
		System.out.println("Number of events processed: " + result.getAccumulatorResult("numEvents"));
		long duration = endTime - startTime;
		int numEventss = result.getAccumulatorResult("numEvents");
		double latency = ((double) duration) / numEventss;
		double throughput = ((double) numEventss) / duration;

		System.out.println("Latency : "+latency);

	}

	public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
		}
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
