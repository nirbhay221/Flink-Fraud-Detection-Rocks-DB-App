package OutofOrderTumblingWithParallelism;


import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;

import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.rocksdb.RocksIterator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class FraudDetectionJob {
	public static final Object rocksDBLock = new Object();

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
		SingleOutputStreamOperator<Tuple3<Long, Double, Long>> keyedTransactions1 = transactions1
				.keyBy(Transaction::getAccountId)
				.process(new TimestampAssigner())
				.name("keyed-transactions-1")
				.setParallelism(2);
		CustomTumblingWindow customTumblingWindow = new CustomTumblingWindow(3000,100);

		keyedTransactions1.print("keyed");
		// Use a custom window assigner to aggregate transactions per account
		SingleOutputStreamOperator<Tuple2<Long, Double>> aggregatedTransactions1 = keyedTransactions1
				.keyBy(0)
				.window(customTumblingWindow)
				.process(new IncrementalSumWindowFunction())
				.name("aggregated-transactions-1")
				.setParallelism(2);

		aggregatedTransactions1.print("aggregaTED" +
				"");
		DataStream<Alert> alerts1 = aggregatedTransactions1
				.keyBy(tuple -> tuple.f0)
				.process(new FraudDetector2())
				.name("fraud-detector-1")
				.setParallelism(2);
		alerts1.print("ALERTS: ");
		alerts1.addSink(new AlertSink()).name("send-alerts-1");


		env.execute("Fraud Detection");


	}

		public static class IncrementalSumWindowFunction extends ProcessWindowFunction<Tuple3<Long, Double, Long>, Tuple2<Long, Double>, Tuple, TimeWindow> {

		private transient RocksDB rocksDB;
		private transient RocksDBManager rocksDBManager;
		private int keyGroup;

		private void openRocksDB(long accountId) {
			try {
				keyGroup = (int) (accountId % 2); // Assuming parallelism of 2

				// Get or open the RocksDB instance
				synchronized (FraudDetectionJob.rocksDBLock) {
					if (rocksDBManager == null) {
						synchronized (FraudDetectionJob.rocksDBLock) {
							rocksDBManager = new RocksDBManager(2); // Assuming parallelism of 2
						}
					}
					rocksDB = rocksDBManager.getOrCreateRocksDBInstance(keyGroup);
				}
			} catch (RocksDBException e) {
				throw new RuntimeException(e);
			}
		}

		private void closeRocksDB() {
			if (rocksDB != null) {
				synchronized (FraudDetectionJob.rocksDBLock) {
					rocksDB.close();
					rocksDB = null;
				}
			}
		}

		@Override
		public void process(Tuple key, Context context, Iterable<Tuple3<Long, Double, Long>> elements, Collector<Tuple2<Long, Double>> out) throws Exception {
			long accountId = key.getField(0);
			openRocksDB(accountId);

			long timestamp = context.window().getStart();
			String compositeKey = timestamp + "_" + accountId;
			byte[] oldValueBytes;

			synchronized (FraudDetectionJob.rocksDBLock) {
				oldValueBytes = rocksDB.get(compositeKey.getBytes());
			}

			double sum = 0;

			for (Tuple3<Long, Double, Long> element : elements) {
				sum += element.f1;
			}

			if (oldValueBytes != null) {
				String oldValueString = new String(oldValueBytes);
				double oldSum = Double.parseDouble(oldValueString);

				sum += oldSum;

				String newValueString = Double.toString(sum);

				synchronized (FraudDetectionJob.rocksDBLock) {
					rocksDB.delete(compositeKey.getBytes());
					rocksDB.put(compositeKey.getBytes(), newValueString.getBytes());
				}
			} else {
				String newValueString = Double.toString(sum);
				synchronized (FraudDetectionJob.rocksDBLock) {
					rocksDB.put(compositeKey.getBytes(), newValueString.getBytes());
				}
			}

			System.out.println("Account ID: "+ accountId+" Sum : "+sum);
			closeRocksDB();
			out.collect(Tuple2.of(accountId, sum));
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

	public static class TimestampAssigner extends KeyedProcessFunction<Long, Transaction, Tuple3<Long, Double, Long>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(Transaction transaction, Context context, Collector<Tuple3<Long, Double, Long>> out) throws Exception {
			out.collect(new Tuple3<>(transaction.getAccountId(), transaction.getAmount(), transaction.getTimestamp()));
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, Double, Long>> out) throws Exception {
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
