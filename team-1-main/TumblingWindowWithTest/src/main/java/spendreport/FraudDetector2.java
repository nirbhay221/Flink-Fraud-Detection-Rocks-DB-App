package spendreport;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;

import java.io.IOException;
public class FraudDetector2 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

    private static final long serialVersionUID = 1L;
    private static final Double THRESHOLD = 10000.0;
    private static final int WINDOW_SIZE = 5; // In seconds

    private transient ValueState<Double> sumState;
    private transient ValueState<Integer> countState;
    private transient Counter fraudCount;
    private transient Gauge latency;
    private transient Gauge throughput;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Double> sumDescriptor = new ValueStateDescriptor<>("sum", Double.class);
        sumState = getRuntimeContext().getState(sumDescriptor);

        ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>("count", Integer.class);
        countState = getRuntimeContext().getState(countDescriptor);

        // Register Prometheus metrics
        fraudCount = Counter.build()
                .name("fraud_detection_count")
                .help("Number of fraud detections")
                .register();

        latency = Gauge.build()
                .name("fraud_detection_latency")
                .help("Latency of the fraud detection process")
                .register();

        throughput = Gauge.build()
                .name("fraud_detection_throughput")
                .help("Throughput of the fraud detection process")
                .register();

        // Start Prometheus HTTP server
        DefaultExports.initialize();
        HTTPServer server = new HTTPServer(8080);
    }

    @Override
    public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> collector) throws Exception {
        long start = System.nanoTime();

        Double currentSum = sumState.value();
        if (currentSum == null) {
            currentSum = 0.0;
        }

        Integer currentCount = countState.value();
        if (currentCount == null) {
            currentCount = 0;
        }

        Double newSum = currentSum + transaction.f1;
        Integer newCount = currentCount + 1;

        sumState.update(newSum);
        countState.update(newCount);

        if (newSum / newCount > THRESHOLD) {
            collector.collect(new Alert());
            sumState.clear();
            countState.clear();
            fraudCount.inc(); // Increment the fraud detection count
        }

        // Update Prometheus metrics
        long end = System.nanoTime();
        latency.set((end - start) / 1000000.0); // Set the fraud detection latency in milliseconds
        throughput.inc(); // Increment the fraud detection throughput
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<Alert> out) throws Exception {
        Double currentSum = sumState.value();
        Integer currentCount = countState.value();
        if (currentCount != null && currentSum != null && currentCount > 0) {
            Double average = currentSum / currentCount;
            if (average > THRESHOLD) {
                out.collect(new Alert());
            }
        }
        sumState.clear();
        countState.clear();
    }
}

