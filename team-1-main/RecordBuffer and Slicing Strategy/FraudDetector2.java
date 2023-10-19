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
public class FraudDetector2 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {
    private static final long serialVersionUID = 1L;
    private final double threshold= 1000;

    public FraudDetector2() {
    }

    @Override
    public void processElement(Tuple2<Long, Double> transaction, Context context, Collector<Alert> out) throws Exception {
        if (transaction.f1 > threshold) {
            out.collect(new Alert());
        }
    }
}
