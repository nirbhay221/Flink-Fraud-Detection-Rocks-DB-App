package spendreport;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Transaction;

class TimestampAssigner extends KeyedProcessFunction<Long, Transaction, Tuple2<Long, Double>> {

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