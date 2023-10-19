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

		// Set event time as the time characteristic
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		// Set the RocksDB parallelism
		DataStream<Transaction> transactions1 = env
				.addSource(new TransactionSource())
				.name("transactions-1")
				.assignTimestampsAndWatermarks(
						new AscendingTimestampExtractor<Transaction>() {
							@Override
							public long extractAscendingTimestamp(Transaction event) {
								return event.getTimestamp();
							}
						});

		transactions1.print("inorder");




		// Use a keyed process function to assign manual timestamps based on the account ID
		DataStream<Tuple2<Long, Double>> keyedTransactions1 = transactions1
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions-1");
		CustomTumblingWindow customTumblingWindow = new CustomTumblingWindow(5000,100);

		// Use a custom window assigner to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions1 =keyedTransactions1
				.keyBy(0)
				.window(customTumblingWindow)
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions-1")
				.setParallelism(1);
		DataStream<Alert> alerts1 = aggregatedTransactions1
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetector2())
				.name("fraud-detector-1")
				.setParallelism(1);

		alerts1.addSink(new AlertSink()).name("send-alerts-1");



			env.execute("Fraud Detection");



	}
	public static class IncrementalSumAggregate implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> createAccumulator() {
			return Tuple2.of(0L, 0.0);
		}

		@Override
		public Tuple2<Long, Double> add(Tuple2<Long, Double> accumulator, Tuple2<Long, Double> value) {
			return Tuple2.of(value.f0, accumulator.f1 + value.f1);
		}

		@Override
		public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
			return accumulator;
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
