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


public class FraudDetector1 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {
    private static final long serialVersionUID = 1L;
    private final double threshold = 1000;
    private long transactionCount = 0;
    private long startTime;

//    private Gauge transactionThroughputCustom;
//    private Gauge transactionLatencyCustom;

    private Gauge transactionThroughputCustom;
    private Gauge transactionLatencyCustom;

//    private HTTPServer server;
//    private CollectorRegistry registry;

    public FraudDetector1(long startTime) {
        this.startTime = startTime;
    }

    @Override
    public void open(Configuration config) throws IOException {

        transactionThroughputCustom = Gauge.build()
                .name("transaction_throughput_custom_tumb_1_parall")
                .help("Number of transactions processed per secondd")
                .register();
//
        transactionLatencyCustom = Gauge.build()
                .name("transaction_latency_custom_tumb_1_parall")
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
            double latency = (double)(endTime - startTime) / 1000;
            double throughput = (double) transactionCount / latency;

            System.out.println("Latency for Custom Tumbling is: " + latency);
            System.out.println("Throughput for Custom Tumbling is: " + throughput);


            transactionLatencyCustom.set(latency);
            transactionThroughputCustom.set(throughput);

//            registry.collect();
//            server.setCollectorRegistry(registry);

            System.out.println("Latency: " + latency + " seconds, throughput: " + throughput + " transactions/second");
        }
    }

    public long getTransactionCount() {
        return transactionCount;
    }
}
