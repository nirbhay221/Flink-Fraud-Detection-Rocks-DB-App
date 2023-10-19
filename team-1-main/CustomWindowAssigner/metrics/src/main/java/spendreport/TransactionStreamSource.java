package CustomWindowAssigner;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.walkthrough.common.entity.Transaction;

import java.util.Random;

public class TransactionStreamSource implements SourceFunction<Transaction> {

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
            long accountId = random.nextInt(10);
            double amount = random.nextDouble() * 1000;
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