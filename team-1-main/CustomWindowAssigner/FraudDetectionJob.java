package spendreport;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
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
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Transaction>() {
					@Override
					public long extractAscendingTimestamp(Transaction event) {

						return event.getTimestamp();
					}
				});


		transactions1.print("inorder");



// Use a keyed process function to assign manual timestamps based on the account ID
		TimestampAssigner TimeStampAssigner1 = new TimestampAssigner();
		DataStream<Tuple2<Long, Double>> keyedTransactions1 = transactions1
				.keyBy(Transaction::getAccountId)
				.process(TimeStampAssigner1)
				.name("keyed-transactions-1");
		long startTime= TimeStampAssigner1.getStartTime();
		CustomTumblingWindow customTumblingWindow = new CustomTumblingWindow(5000,100);
keyedTransactions1.print("keyed-transaction");
		// Use a custom window assigner to aggregate transactions per account
		DataStream<Tuple2<Long, Double>> aggregatedTransactions1 =keyedTransactions1
				.keyBy(0)
				.window(new CustomSlidingWindow(3000,100))
				.aggregate(new IncrementalMeanAggregate())
				.name("aggregated-transactions-1")
				.setParallelism(1);
		aggregatedTransactions1.print("aggregated-transaction");
		FraudDetector2 fraudDetector = new FraudDetector2(startTime);
		DataStream<Alert> alerts1 = aggregatedTransactions1
				.keyBy(tuple -> tuple.f0)
				.process(fraudDetector)
				.name("fraud-detector-1")
				.setParallelism(1);
		alerts1.print("alerts");

alerts1.print();
		alerts1.addSink(new AlertSink()).name("send-alerts-1");
//
//		long numTransactions = fraudDetector.getTransactionCount();
//		double latency = (double)(endTime - startTime) / 1000;
//		double throughput = (double) numTransactions / latency;
		// Add the PrometheusReporter
//		transactionLatency.set(latency);
//		transactionThroughput.set(throughput);
		env.execute("Fraud Detection");

	}
	public static class IncrementalSumAggregate implements AggregateFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> createAccumulator() {
			return Tuple2.of(0L, 0.0);
		}

		@Override
		public Tuple2<Long, Double> add(Tuple2<Long, Double> accumulator, Tuple2<Long, Double> value) {
			return Tuple2.of(accumulator.f0, accumulator.f1 + value.f1);
		}


		@Override
		public Tuple2<Long, Double> getResult(Tuple2<Long, Double> accumulator) {
			return accumulator;
		}

		@Override
		public Tuple2<Long, Double> merge(Tuple2<Long, Double> a, Tuple2<Long, Double> b) {
			return Tuple2.of(a.f0, a.f1 + b.f1);
		}

		public TypeInformation<Tuple2<Long, Double>> getAccumulatorType() {
			return new TupleTypeInfo<>(TypeInformation.of(Long.class), TypeInformation.of(Double.class));
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
	public class CustomWindowFunction implements WindowFunction<Double, Tuple2<String, Double>, Tuple, TimeWindow> {

		@Override
		public void apply(Tuple key, TimeWindow window, Iterable<Double> input, Collector<Tuple2<String, Double>> out) throws Exception {
			double mean = 0.0;
			int count = 0;
			for (Double value : input) {
				mean += value;
				count++;
			}
			mean /= count;
			out.collect(new Tuple2<>("window_" + window.getEnd(), mean));
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
		long StartTime = 0 ;

		@Override
		public void processElement(Transaction transaction, Context context, Collector<Tuple2<Long, Double>> out) throws Exception {
			StartTime = System.currentTimeMillis();
			out.collect(new Tuple2<>(transaction.getAccountId(), transaction.getAmount()));
		}
		public long getStartTime(){
			return StartTime;
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
