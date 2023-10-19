package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;
public class CustomTumblingWindow extends WindowAssigner<Object, TimeWindow> {

    private final long size;
    private final boolean enableSlicing;
    private final long maxWindowsPerAssign;
    private transient PriorityQueue<Window> priorityQueue;

    public CustomTumblingWindow(long size, boolean enableSlicing, long maxWindowsPerAssign) {
        this.size = size;
        this.enableSlicing = enableSlicing;
        this.maxWindowsPerAssign = maxWindowsPerAssign;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        long windowStart = timestamp - (timestamp % size);
        long windowEnd = windowStart + size;
        List<TimeWindow> windows = new ArrayList<>();

        if (enableSlicing) {
            while (windowEnd <= context.getCurrentProcessingTime()) {
                windows.add(new TimeWindow(windowStart, windowEnd));
                windowStart += size;
                windowEnd += size;

                // Print account id and value for each slice
                System.out.println("Slice: " + windows.size() + ", Account ID: " + ((Tuple2<String, Double>) element).f0 + ", Value: " + ((Tuple2<String, Double>) element).f1);
            }
        } else {
            windows.add(new TimeWindow(windowStart, windowEnd));
        }

        if (priorityQueue == null) {
            priorityQueue = new PriorityQueue<>(Math.toIntExact(maxWindowsPerAssign), Comparator.comparingLong(Window::getEnd));
        }

        for (TimeWindow window : windows) {
            Window w = new Window(window);
            if (priorityQueue.size() < maxWindowsPerAssign) {
                priorityQueue.offer(w);
            } else {
                Window earliestWindow = priorityQueue.poll();
                earliestWindow.clear();
                priorityQueue.offer(w);
            }
        }

        return windows;
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
        return "CustomTumblingWindow(" + size + ")";
    }

    private class Window {
        private final TimeWindow window;
        private final List<Object> buffer = new ArrayList<>();

        public Window(TimeWindow window) {
            this.window = window;
        }

        public long getEnd() {
            return window.maxTimestamp();
        }

        public void add(Object element) {
            buffer.add(element);
        }

        public void clear() {
            buffer.clear();
        }
    }
}
