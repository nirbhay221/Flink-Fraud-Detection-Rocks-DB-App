package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import sun.print.CustomMediaTray;

import java.util.*;
import java.util.concurrent.ScheduledFuture;

public class CustomSlidingWindow extends WindowAssigner<Object, TimeWindow> {

    private final long size;
    private final long slide;
    private final boolean enableSlicing;
    private final long maxWindowsPerAssign;
    private final int maxQueueSize;
    private transient long lastWindowStart;
    private transient Queue<Object> elementQueue;

    public CustomSlidingWindow(long size, long slide, boolean enableSlicing, long maxWindowsPerAssign, int maxQueueSize) {
        this.size = size;
        this.slide = slide;
        this.enableSlicing = enableSlicing;
        this.maxWindowsPerAssign = maxWindowsPerAssign;
        this.maxQueueSize = maxQueueSize;
        this.lastWindowStart = Long.MIN_VALUE;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        if (elementQueue == null) {
            elementQueue = new ArrayDeque<>(maxQueueSize);
        }
        if (elementQueue.size() >= maxQueueSize) {
            throw new RuntimeException("Queue size limit exceeded: " + maxQueueSize);
        }
        elementQueue.offer(element);

        List<TimeWindow> windows = new ArrayList<>();

        while (!elementQueue.isEmpty()) {
            Object queuedElement = elementQueue.poll();
            long windowStart = timestamp - (timestamp % slide);
            long windowEnd = windowStart + size;

            if (enableSlicing) {
                while (windowEnd <= context.getCurrentProcessingTime()) {
                    if (windowStart >= lastWindowStart + slide) {
                        windows.add(new TimeWindow(windowStart, windowEnd));
                        lastWindowStart = windowStart;
                    }
                    windowStart += slide;
                    windowEnd += slide;
                }
            } else {
                if (windowStart >= lastWindowStart + slide) {
                    windows.add(new TimeWindow(windowStart, windowEnd));
                    lastWindowStart = windowStart;
                }
            }
        }

        if (windows.size() > maxWindowsPerAssign) {
            windows = windows.subList(windows.size() - (int) maxWindowsPerAssign, windows.size());
            lastWindowStart = windows.get(0).getStart();
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
        return true;
    }

    @Override
    public String toString() {
        return "CustomSlidingWindow(" + size + ", " + slide + ")";
    }
}
