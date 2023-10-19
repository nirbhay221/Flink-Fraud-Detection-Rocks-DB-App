package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;


public class CustomSlidingWindow extends WindowAssigner<Object, TimeWindow> {

    private final long size;
    private final long slide;
    private final boolean enableSlicing;

    public CustomSlidingWindow(long size, long slide, boolean enableSlicing) {
        this.size = size;
        this.slide = slide;
        this.enableSlicing = enableSlicing;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssigner.WindowAssignerContext context) {
        long windowStart = timestamp - (timestamp % slide);
        long windowEnd = windowStart + size;

        if (enableSlicing) {
            List<TimeWindow> windows = new ArrayList<>();
            while (windowEnd <= context.getCurrentProcessingTime()) {
                windows.add(new TimeWindow(windowStart, windowEnd));
                windowStart += slide;
                windowEnd += slide;
            }
            return windows;
        } else {
            return Collections.singletonList(new TimeWindow(windowStart, windowEnd));
        }
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return EventTimeTrigger.create();
    }

    @Override
    public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
        return new TimeWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
        return false;
    }

    @Override
    public String toString() {
        return "CustomSlidingWindow(" + size + ", " + slide + ")";
    }
}

