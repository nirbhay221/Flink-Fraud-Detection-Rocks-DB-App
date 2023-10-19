package OutofOrderTumblingWindow;

//import io.prometheus.client.Counter;
//import io.prometheus.client.Gauge;
//import io.prometheus.client.exporter.HTTPServer;
//import io.prometheus.client.hotspot.DefaultExports;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.walkthrough.common.entity.Alert;
public class FraudDetector2 extends KeyedProcessFunction<Long, Tuple2<Long, Double>, Alert> {

    private static final long serialVersionUID = 1L;

    // The threshold value above which an account is considered fraudulent
    private static final double FRAUD_THRESHOLD = 10000.0;

    @Override
    public void processElement(Tuple2<Long, Double> value, Context ctx, Collector<Alert> out) throws Exception {
        long accountId = value.f0;
        double sum = value.f1;

        if (sum > FRAUD_THRESHOLD) {
            Alert alert = new Alert();
            out.collect(alert);
        }
    }
}
