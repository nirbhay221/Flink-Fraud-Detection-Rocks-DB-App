package spendreport;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.contrib.streaming.state.*;
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
import java.time.Duration;
import java.util.Collection;


public class FraudDetectionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		long checkpointInterval = 5000;
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/seednew/Downloads/workspace/frauddetection/rocksdb_data");
		rocksDBStateBackend.setNumberOfTransferThreads(4);
		rocksDBStateBackend.setNumberOfTransferingThreads(4);
		// Flink execution environment and set the state backend
		//env.setStateBackend(rocksDBStateBackend); add it 
		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Set the RocksDB parallelism
		env.getConfig().setParallelism(4);

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
				.name("keyed-transactions").setParallelism(1);
	;

		// Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions = keyedTransactions
				.keyBy(0)
				.window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions")
				.setParallelism(1)
			;
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
		env.execute("Fraud Detection");
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
