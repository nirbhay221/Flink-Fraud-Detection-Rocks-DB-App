package spendreport;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
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
import java.time.Duration;

public class FraudDetectionJob {
	public static SinkFunction out = null;
	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		long checkpointInterval = 5000;
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/user/Desktop/Stream-CS551/project/team-1/TumblingWindows/rocksdb_data");

		// Create a Flink execution environment and set the state backend
		env.setStateBackend(rocksDBStateBackend);
		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// Add the transaction source
		DataStream<Transaction> transactions = env
				.addSource(new TransactionSource())
				.name("transactions")
				.assignTimestampsAndWatermarks(WatermarkStrategy
						.<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
						.withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactions = transactions
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions");

		// Use a tumbling window of 5 minutes to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions = keyedTransactions
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.minutes(5)))
				.sum(1);

		// Apply the fraud detection logic on the aggregated transactions
		DataStream<Alert> alerts = aggregatedTransactions
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetector())
				.name("fraud-detector");

		// Send the alerts to the alert sink
		alerts
				.addSink(new AlertSink())
				.name("send-alerts");

		printOrTest(aggregatedTransactions);

		// Execute the Flink job
		env.execute("Fraud Detection");
	}

	public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
		if (out == null) {
			ds.print();
		} else {
			ds.addSink(out);
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
	}}
