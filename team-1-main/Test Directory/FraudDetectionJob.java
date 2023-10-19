package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class FraudDetectionJob {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		long checkpointInterval = 5000;
		env.enableCheckpointing(checkpointInterval);
		RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("file:///home/seednew/Downloads/workspace/frauddetection/rocksdb_data");

		// Create a Flink execution environment and set the state backend
		env.setStateBackend(rocksDBStateBackend);
		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		rocksDBStateBackend.setNumberOfTransferThreads(1);
		rocksDBStateBackend.setNumberOfTransferingThreads(1);
		// Set the RocksDB parallelism

		// Create the two streams of transactions
		DataStream<Tuple2<Long, Double>> flinkTransactions = env
				.addSource(new DuplicateTransactionSource())
				.name("flink-transactions")
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Double>>forMonotonousTimestamps()
						.withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()));
		flinkTransactions.print("Flink-Transaction");
		DataStream<Tuple2<Long, Double>> customTransactions = env
				.addSource(new DuplicateTransactionSource())
				.name("custom-transactions")
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<Long, Double>>forMonotonousTimestamps()
						.withTimestampAssigner((event, timestamp) -> System.currentTimeMillis()));
		customTransactions.print("Custom-Transaction");
		// Apply Flink's tumbling window to the first stream
		DataStream<Tuple2<Long, Double>> flinkAggregated = flinkTransactions
				.keyBy(0)
				.window(TumblingEventTimeWindows.of(Time.seconds(5)))
				.sum(1)
				.name("flink-aggregated");

		// Apply the custom tumbling window to the second stream
		DataStream<Tuple2<Long, Double>> customAggregated = customTransactions
				.keyBy(0)
				.window(new CustomTumblingWindow(Time.seconds(5).toMilliseconds(), true, 2))
				.aggregate(new IncrementalSumAggregate())
				.name("custom-aggregated");

		// Print the output of both streams for validation
		customAggregated.print("custom-aggregated");

		// Execute the Flink job
		env.execute("Fraud Detection");
	}

	public static class DuplicateTransactionSource implements SourceFunction<Tuple2<Long, Double>> {
		private volatile boolean isRunning = true;

		@Override
		public void run(SourceFunction.SourceContext<Tuple2<Long, Double>> sourceContext) throws Exception {
			// Emit duplicate transactions for each account at fixed intervals
			while (isRunning) {
				for (int i = 1; i <= 5; i++) {
					sourceContext.collect(Tuple2.of((long) i, 100.0));
				}
				Thread.sleep(1000);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}

	public static class IncrementalSumAggregate implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> createAccumulator() {
			return Tuple2.of(0L, 0.0);
		}

		@Override
		public Tuple2<Long, Double> add(Tuple2<Long, Double> value, Tuple2<Long, Double> accumulator) {
			return Tuple2.of(value.f0, value.f1 + accumulator.f1);
		}

		@Override
		public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
			return Tuple2.of(accumulator.f0, accumulator.f1);
		}

		@Override
		public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
			return Tuple2.of(a.f0, a.f1 + b.f1);
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


	public static class CustomWindow extends WindowAssigner<Object, TimeWindow> {

		private final long size;
		private final long slide;

		public CustomWindow(long size, long slide) {
			this.size = size;
			this.slide = slide;
		}

		@Override
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
			long startTime = timestamp - (timestamp % slide);
			long endTime = startTime + size;
			List<TimeWindow> windows = new ArrayList<>();
			while (endTime <= context.getCurrentProcessingTime()) {
				windows.add(new TimeWindow(startTime, endTime));
				startTime += slide;
				endTime += slide;
			}
			return windows;
		}

		@Override
		public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
			return EventTimeTrigger.create();
		}

		@Override
		public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
			return new TimeWindow.Serializer();
		}

		@Override
		public boolean isEventTime() {
			return false;
		}

		@Override
		public String toString() {
			return "CustomWindow(" + size + ", " + slide + ")";
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
	public static class TimestampAssigner2 extends KeyedProcessFunction<Long, Transaction, Tuple2<Long, Double>> {

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


}
