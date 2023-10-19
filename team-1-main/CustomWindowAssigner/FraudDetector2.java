package spendreport;

import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.io.IOException;

public class FraudDetector2 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {
    private static final long serialVersionUID = 1L;
    private final double threshold = 1000;
    private long transactionCount = 0;
    private long startTime;

    Gauge transactionThroughput;
    Gauge transactionLatency;

    public FraudDetector2(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void open(Configuration config) throws IOException {

        transactionThroughput = Gauge.build()
                .name("transaction_throughput")
                .help("Number of transactions processed per second")
                .register();
//
        transactionLatency = Gauge.build()
                .name("transaction_latency")
                .help("Latency of transaction processing")
                .register();
//
        DefaultExports.initialize();
        HTTPServer server = new HTTPServer(8080);
//
//
    }

    @Override
    public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> out) throws Exception {

        transactionCount++;
        if (transaction.f1 > threshold) {
            out.collect(new Alert());
        }
        if (transactionCount % 100 == 0) {
            long endTime = System.currentTimeMillis();
            double latency = (double)(endTime - startTime) / 1000;
            double throughput = (double) transactionCount / latency;

            transactionLatency.set(latency);
            transactionThroughput.set(throughput);


            System.out.println("Latency: " + latency + " seconds, throughput: " + throughput + " transactions/second");
        }
    }

    public long getTransactionCount() {
        return transactionCount;
    }
}
