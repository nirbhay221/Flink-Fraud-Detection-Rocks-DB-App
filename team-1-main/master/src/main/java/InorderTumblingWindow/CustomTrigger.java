package InorderTumblingWindow;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
public class CustomTrigger<W extends Window> extends Trigger<Tuple2<Long, Double>, W> {

    private final long maxCount;

    public CustomTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onElement(Tuple2<Long, Double> element, long timestamp, W window, TriggerContext ctx) throws Exception {
        // Compute incremental mean aggregation of incoming elements
        ValueState<Double> sumState = ctx.getPartitionedState(new ValueStateDescriptor<>("sum", Double.class, 0.0));
        ValueState<Long> countState = ctx.getPartitionedState(new ValueStateDescriptor<>("count", Long.class, 0L));
        double sum = sumState.value();
        long count = countState.value();
        double value = element.f1;
        sum += value;
        count++;
        sumState.update(sum);
        countState.update(count);

        // Check if maximum count has been reached or if event time timer has expired
        if (count >= maxCount || timestamp == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // Register event time timer for the end of the window
            ctx.registerEventTimeTimer(window.maxTimestamp());
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) throws Exception {
        // Fire and purge if event time timer has expired
        if (time == window.maxTimestamp()) {
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        // Clear state and delete event time timer
        ctx.getPartitionedState(new ValueStateDescriptor<>("sum", Double.class, 0.0)).clear();
        ctx.getPartitionedState(new ValueStateDescriptor<>("count", Long.class, 0L)).clear();
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    public static <W extends Window> CustomTrigger<W> of(long maxCount) {
        return new CustomTrigger<>(maxCount);
    }
}
