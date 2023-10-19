import InorderSlidingWindow.CustomSlidingWindow;
//import CustomWindowAssigner.TransactionStreamSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.junit.Test;
import InorderSlidingWindow.FraudDetectionJob;

import java.util.Random;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CustomSlidingWindowTest {


    @Test
    public void testCustomTumblingWindowSlices() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // Create a transaction stream
//        DataStream<Transaction> transactions = env.addSource(new TransactionSource())
//                .name("transactions");

        DataStream<Transaction> transactions = env.addSource(new TransactionStreamSource(10))
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


        // Use a sliding window of 5 minutes with a slide of 1 minute to aggregate transactions per account
        DataStream<Tuple2<Long, Double>> defaultAggregatedTransactions = keyedTransactions
                .keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1))) // windowSize, change the first metric for different window sizes
                .aggregate(new FraudDetectionJob.IncrementalMeanAggregate())
                .name("aggregated-transactions")
                .setParallelism(1)
                ;


        defaultAggregatedTransactions.print("Default Aggregated Transaction: ");

        // Use a custom tumbling window assigner to aggregate transactions per account
        DataStream<Tuple2<Long, Double>> customAggregatedTransactions =keyedTransactions
                .keyBy(0)
                .window(new CustomSlidingWindow(300000, 60000))
                .aggregate(new FraudDetectionJob.IncrementalSumAggregate())
                .name("aggregated-transactions-1")
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

        env.execute();

        // Check if both result lists have the same elements
        assertEquals(defaultResults, customResults);
    }

//    private class TransactionStreamSource {
//        public TransactionStreamSource(int i) {
//        }
//    }




    public static class TransactionStreamSource implements SourceFunction<Transaction> {

        private volatile boolean isRunning = true;
        private final int maxTransactions;
        private final Random random = new Random();
        private int numTransactions = 0;


        public TransactionStreamSource(int maxTransactions) {
            this.maxTransactions = maxTransactions;
        }


        @Override
        public void run(SourceContext<Transaction> ctx) throws Exception {
            while (isRunning && numTransactions < maxTransactions) {
//                long accountId = random.nextInt(10);
//                double amount = random.nextDouble() * 1000;
                int accountId = random.nextInt(5) + 1;
                double amount = random.nextInt(100) + 1;
                long timestamp = System.currentTimeMillis();

                Transaction transaction = new Transaction(accountId, (long) amount, timestamp);
                ctx.collect(transaction);

                numTransactions ++;
                Thread.sleep(100);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

    }
}
