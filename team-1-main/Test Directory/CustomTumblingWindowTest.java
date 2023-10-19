import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.source.TransactionSource;
import org.junit.Test;
import spendreport.CustomTumblingWindow;
import spendreport.FraudDetectionJob;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CustomTumblingWindowTest {

//    @Test
//    public void testCustomTumblingWindowSlices() throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        // Create a transaction stream
//        DataStream<Transaction> transactions = env.addSource(new TransactionSource())
//                .name("transactions");
//
//        // Assign timestamps and watermarks
//        transactions = transactions
//                .assignTimestampsAndWatermarks(WatermarkStrategy
//                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));
//
//        // Use a keyed process function to assign manual timestamps based on the account ID
//        DataStream<Tuple2<Long, Double>> keyedTransactions = transactions
//                .keyBy(Transaction::getAccountId)
//                .process(new FraudDetectionJob.TimestampAssigner())
//                .name("keyed-transactions");
//
//        // Use a default tumbling event-time window to aggregate transactions per account
//        DataStream<Tuple2<Long, Double>> defaultAggregatedTransactions = keyedTransactions
//                .keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
//                .name("default-aggregated-transactions")
//                .setParallelism(1);
//
//        // Use a custom tumbling window assigner to aggregate transactions per account
//        DataStream<Tuple2<Long, Double>> customAggregatedTransactions = keyedTransactions
////                .keyBy(0)
////                .window(new CustomTumblingWindow(Time.minutes(1).toMilliseconds(), false))
////                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
////                .name("custom-aggregated-transactions")
////                .setParallelism(1);
//                .keyBy(0)
//                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
//                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
//                .name("default-aggregated-transactions")
//                .setParallelism(1);
//
//
//        // Collect results from both windows
//        List<Tuple2<Long, Double>> defaultResults = new ArrayList<>();
//        List<Tuple2<Long, Double>> customResults = new ArrayList<>();
//
//        defaultAggregatedTransactions
//                .flatMap(new FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    @Override
//                    public void flatMap(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out) throws Exception {
//                        out.collect(value);
//                    }
//                })
//                .process(new ProcessFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    @Override
//                    public void processElement(Tuple2<Long, Double> value, Context ctx, Collector<Tuple2<Long, Double>> out) throws Exception {
//                        defaultResults.add(value);
//                    }
//                })
//                .name("default-result-collector")
//                .setParallelism(1);
//
//        customAggregatedTransactions
//                .flatMap(new FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    @Override
//                    public void flatMap(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out) throws Exception {
//                        out.collect(value);
//                    }
//                }).process(new ProcessFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
//                    @Override
//                    public void processElement(Tuple2<Long, Double> value, Context ctx, Collector<Tuple2<Long, Double>> out) throws Exception {
//                        customResults.add(value);
//                    }
//                })
//                .name("default-result-collector")
//                .setParallelism(1);
//
//
//
//        env.execute();
//
//
//        assertEquals(defaultResults,customResults);
//    }


    @Test
    public void testCustomTumblingWindowSlices() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a transaction stream
        DataStream<Transaction> transactions = env.addSource(new TransactionSource())
                .name("transactions");

        // Assign timestamps and watermarks
        transactions = transactions
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp()));

        // Use a keyed process function to assign manual timestamps based on the account ID
        DataStream<Tuple2<Long, Double>> keyedTransactions = transactions
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetectionJob.TimestampAssigner())
                .name("keyed-transactions");

        keyedTransactions.print("Keyed Transaction: ");

        // Use a default tumbling event-time window to aggregate transactions per account
        DataStream<Tuple2<Long, Double>> defaultAggregatedTransactions = keyedTransactions
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
                .name("default-aggregated-transactions")
                .setParallelism(1);

        defaultAggregatedTransactions.print("Default Aggregated Transaction: ");

        // Use a custom tumbling window assigner to aggregate transactions per account
        DataStream<Tuple2<Long, Double>> customAggregatedTransactions = keyedTransactions
                .keyBy(0)
                .window(new CustomTumblingWindow(Time.minutes(5).toMilliseconds(), false, 10))
                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
                .name("custom-aggregated-transactions")
                .setParallelism(1);

        customAggregatedTransactions.print("Custom Aggregated Transaction: ");

        // Collect results from both windows
        List<Tuple2<Long, Double>> defaultResults = new ArrayList<>();
        List<Tuple2<Long, Double>> customResults = new ArrayList<>();

        defaultAggregatedTransactions
                .flatMap(new FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void flatMap(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out) throws Exception {
                        out.collect(value);
                    }
                })
                .process(new ProcessFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void processElement(Tuple2<Long, Double> value, Context ctx, Collector<Tuple2<Long, Double>> out) throws Exception {
                        defaultResults.add(value);
                    }
                })
                .name("default-result-collector")
                .setParallelism(1);

        customAggregatedTransactions
                .flatMap(new FlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void flatMap(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out) throws Exception {
                        out.collect(value);
                    }
                })
                .process(new ProcessFunction<Tuple2<Long, Double>, Tuple2<Long, Double>>() {
                    @Override
                    public void processElement(Tuple2<Long, Double> value, Context ctx, Collector<Tuple2<Long, Double>> out) throws Exception {
                        customResults.add(value);
                    }
                })
                .name("custom-result-collector")
                .setParallelism(1);

//        env.execute();

        // execute the Flink job asynchronously with a 30-second timeout
        CompletableFuture<Void> jobFuture = (CompletableFuture<Void>) env.executeAsync();
        try {
            jobFuture.get(30, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // the job has timed out
            jobFuture.cancel(true);
            fail("The Flink job timed out.");
        }

//        for (Tuple2<Long, Double> result : defaultResults) {
//            System.out.println(result.f0 + ": " + result.f1);
//        }
//
//        for (Tuple2<Long, Double> result : customResults) {
//            System.out.println(result.f0 + ": " + result.f1);
//        }

        // Check if both result lists have the same elements
        assertEquals(defaultResults, customResults);
    }









}