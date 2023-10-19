package spendreport;

import io.prometheus.client.CollectorRegistry;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import java.util.ArrayList;
import java.util.List;

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


public class FraudDetectorInorderTumbling extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

    private static final long serialVersionUID = 1L;
    private static final Double THRESHOLD = 10000.0;
    private static final int WINDOW_SIZE = 5; // In seconds

    private transient ValueState<Double> sumState;
    private transient ListState<Tuple2<Long, Double>> recordBufferState;

    private transient Gauge transactionThroughputInorder;
    private transient Gauge transactionLatencyInorder;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);



//                CollectorRegistry.defaultRegistry.unregister(transactionThroughputInorder);
//        CollectorRegistry.defaultRegistry.unregister(transactionLatencyInorder);
// Register Prometheus metrics
        transactionThroughputInorder = Gauge.build()
                .name("transaction_throughput_for_inorder_tumbling")
                .help("Number of transactions processed per second")
                .register();

        transactionLatencyInorder = Gauge.build()
                .name("transaction_latency_inorder_tumbling")
                .help("Latency of transaction processing")
                .register();

//        CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry;
//        if (collectorRegistry.getMetricNames().contains(transactionThroughputInorder.name)) {
//            collectorRegistry.unregister(transactionThroughputInorder);
//        }
//        if (collectorRegistry.getNames().contains(transactionLatencyInorder.name)) {
//            collectorRegistry.unregister(transactionLatencyInorder);
//        }
//
//        // Register Prometheus metrics
//        collectorRegistry.register(transactionThroughputInorder);
//        collectorRegistry.register(transactionLatencyInorder);
//
//        CollectorRegistry.defaultRegistry.register(transactionThroughputInorder);
//        CollectorRegistry.defaultRegistry.register(transactionLatencyInorder);

        DefaultExports.initialize();
        HTTPServer server = new HTTPServer(8082);


        // Initialize state
        ValueStateDescriptor<Double> sumStateDescriptor = new ValueStateDescriptor<>(
                "transaction-sum",
                Double.class
        );
        sumState = getRuntimeContext().getState(sumStateDescriptor);

        ListStateDescriptor<Tuple2<Long, Double>> recordBufferStateDescriptor = new ListStateDescriptor<>(
                "record-buffer",
                TypeInformation.of(new TypeHint<Tuple2<Long, Double>>() {})
        );
        recordBufferState = getRuntimeContext().getListState(recordBufferStateDescriptor);


        CollectorRegistry.defaultRegistry.unregister(transactionThroughputInorder);
        CollectorRegistry.defaultRegistry.unregister(transactionLatencyInorder);
    }

    @Override
    public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> out) throws Exception {
        long startTime =  System.nanoTime();

        // Update sum state
        Double currentSum = sumState.value();
        if (currentSum == null) {
            currentSum = 0.0;
        }
        currentSum += transaction.f1;
        sumState.update(currentSum);

        // Add record to buffer
        recordBufferState.add(transaction);

        // Set timer to trigger window
        context.timerService().registerEventTimeTimer(context.timestamp() + WINDOW_SIZE * 1000);

        // Update Prometheus metrics
        transactionThroughputInorder.inc();
        long end = System.nanoTime();
        transactionLatencyInorder.set((end - startTime) / 1000000.0); // Set the fraud detection latency in milliseconds


    }
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) throws Exception {
        // Retrieve records from buffer that fall within window interval bounds
        List<Tuple2<Long, Double>> records = new ArrayList<>();
        for (Tuple2<Long, Double> record : recordBufferState.get()) {
            if (record.f0 >= timestamp - WINDOW_SIZE * 1000 && record.f0 < timestamp) {
                records.add(record);
            }
        }

        // Compute sum of transactions in window
        Double sum = 0.0;
        for (Tuple2<Long, Double> record : records) {
            sum += record.f1;
        }

        // Check if sum exceeds threshold
        if (sum > THRESHOLD) {
            out.collect(new Alert());
        }

        // Clear buffer
        recordBufferState.clear();

        // Clear sum state
        sumState.clear();
    }
}

