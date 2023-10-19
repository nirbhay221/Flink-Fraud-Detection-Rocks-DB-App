package spendreport;import java.util.List;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
public class CustomWindowFunction implements WindowFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple, TimeWindow> {

    private final CustomSlidingWindow assigner;

    public CustomWindowFunction(CustomSlidingWindow assigner) {
        this.assigner = assigner;
    }

    @Override
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple2<Long, Double>> input, Collector<Tuple2<Long, Double>> out) {
        double sum = 0;
        int count = 0;
        for (Tuple2<Long, Double> element : input) {
            sum += element.f1;
            count++;
        }
        double mean = count > 0 ? sum / count : 0;
        out.collect(new Tuple2<>(key.getField(0), mean));
    }

}
