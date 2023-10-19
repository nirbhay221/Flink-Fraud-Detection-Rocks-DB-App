package spendreport;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import spendreport.FraudDetector;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;

public class FraudDetectionJob {
	public static SinkFunction out = null;

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		long checkpointInterval = 5000;
		Counter numCheckpoints = new Counter();
		Counter numEvents = new Counter();
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/user/Desktop/Stream-CS551/project/team-1/UpdatedTumblingWindows/rocksdb_data");
		rocksDBStateBackend.setNumberOfTransferThreads(4);
		rocksDBStateBackend.setNumberOfTransferingThreads(4);

		// Create a Flink execution environment and set the state backend
		env.setStateBackend(rocksDBStateBackend);
		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		env.getConfig().setParallelism(1);

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

		// Use a tumbling window of 5 minutes to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions = keyedTransactions
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))// change this to check for different window sizes
				.aggregate(new TransactionAggregator())
				.name("aggregated-transactions").setParallelism(1);

		// Apply the fraud detection logic on the aggregated transactions
		DataStream<Alert> alerts = aggregatedTransactions
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetector())
				.name("fraud-detector").setParallelism(1);

		// Send the alerts to the alert sink
		alerts
				.addSink(new AlertSink())
				.name("send-alerts").setParallelism(1);
		// Execute the Flink job

		printOrTest(aggregatedTransactions);

		long startTime = System.currentTimeMillis();
		JobExecutionResult result =env.execute("Fraud Detection");
		long endTime = System.currentTimeMillis();
		System.out.println("Number of checkpoints: " + result.getAccumulatorResult("numCheckpoints"));
		System.out.println("Number of events processed: " + result.getAccumulatorResult("numEvents"));
		long duration = endTime - startTime;
		int numEventss = result.getAccumulatorResult("numEvents");
		double latency = ((double) duration) / numEventss;
		double throughput = ((double) numEventss) / duration;

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
			// Do nothing
		}
	}

	public static class TransactionAggregator implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> createAccumulator() {
			return new Tuple2<>(null, 0.0);
		}

		@Override
		public Tuple2<Long, Double> add(Tuple2<Long, Double> value, Tuple2<Long, Double> accumulator) {
			if (accumulator.f0 == null) {
				accumulator.f0 = value.f0;
			}
			accumulator.f1 += value.f1;
			return accumulator;
		}

		@Override
		public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
			throw new UnsupportedOperationException("Merge not supported");
		}
	}

}
