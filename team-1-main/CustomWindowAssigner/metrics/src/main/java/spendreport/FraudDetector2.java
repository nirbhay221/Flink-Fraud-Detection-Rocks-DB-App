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
import io.prometheus.client.CollectorRegistry;


public class FraudDetector2 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {
    private static final long serialVersionUID = 1L;
    private final double threshold = 1000;
    private long transactionCount = 0;
    private long startTime;

//    private Gauge transactionThroughputDefault;
//    private Gauge transactionLatencyDefault;

    private Gauge transactionThroughputDefaultSlid;
    private Gauge transactionLatencyDefaultSlid;

//    private HTTPServer server;
//    private CollectorRegistry registry;

    public FraudDetector2(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void open(Configuration config) throws IOException {

        transactionThroughputDefaultSlid = Gauge.build()
                .name("transaction_throughput_custom_tumb_2_parall")
                .help("Number of transactions processed per secondd")
                .register();
//
        transactionLatencyDefaultSlid = Gauge.build()
                .name("transaction_latency_custom_tumb_2_parall")
                .help("Latency of transaction processingg")
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
            System.out.println("EndTime: " + endTime);
            System.out.println("StartTime: " + startTime);
            double latency = (double)(endTime - startTime) / 1000;
            double throughput = (double) transactionCount / latency;

            System.out.println("Latency for Custom Sliding 2 is: " + latency);
            System.out.println("Throughput for Custom Sliding 2 is: " + throughput);

            transactionLatencyDefaultSlid.set(latency);
            transactionThroughputDefaultSlid.set(throughput);

//
//            registry.collect();
//            server.setCollectorRegistry(registry);

            System.out.println("Latency: " + latency + " seconds, throughput: " + throughput + " transactions/second");
        }
    }

    public long getTransactionCount() {
        return transactionCount;
    }
}
