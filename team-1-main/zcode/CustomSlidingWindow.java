package CustomWindowAssigner;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

import java.util.*;
public class CustomSlidingWindow extends WindowAssigner<Object, TimeWindow> {

    private final long size;
    private final long slide;
    private final long paneSize;
    private final Map<Long, List<Double>> paneElementsMap;

    public CustomSlidingWindow(long size, long slide) {
        this.size = size;
        this.slide = slide;
        this.paneSize = gcd(size, slide);
        this.paneElementsMap = new HashMap<>();
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        List<TimeWindow> panes = new ArrayList<>();
        long paneId = getPaneId(timestamp);
        long elementEnd = (paneId + 1) * paneSize;
        long paneStart = elementEnd - paneSize;
        while (paneStart + slide > timestamp) {
            panes.add(new TimeWindow(paneStart, elementEnd));
            paneStart -= slide;
            elementEnd -= slide;
        }
        addToPane(element, timestamp, paneId);
        return panes;
    }

    @Override
    public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return new CustomTrigger(size / slide);
    }

    private long getPaneId(long timestamp) {
        return timestamp / paneSize;
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

    public void onElement(Object element, long timestamp, TimeWindow window, WindowAssignerContext context) throws Exception {
        addToPane(element, timestamp, getPaneId(timestamp));
    }

    public void onProcessingTime(long time, TimeWindow window, Trigger.TriggerContext context, Collector<Object> out) throws Exception {
        combinePanes(getWindowPaneIds(window), out);
    }

    public void onEventTime(long time, TimeWindow window, Trigger.TriggerContext context, Collector<Object> out) throws Exception {
        combinePanes(getWindowPaneIds(window), out);
    }

    private void addToPane(Object element, long timestamp, long paneId) {
        if (element instanceof Tuple2) {
            Tuple2<?, ?> tuple2 = (Tuple2<?, ?>) element;
            element = tuple2.f1;
        }
        paneElementsMap.computeIfAbsent(paneId, k -> new ArrayList<>()).add((Double) element);
    }


    private void combinePanes(List<Long> paneIds, Collector<Object> out) {
        List<Double> combinedPaneElements = new ArrayList<>();
        for (long paneId : paneIds) {
            List<Double> paneElements = paneElementsMap.remove(paneId);
            if (paneElements != null) {
                combinedPaneElements.addAll(paneElements);
            }
        }
        if (!combinedPaneElements.isEmpty()) {
            double sum = 0.0;
            for (Double element : combinedPaneElements) {
                sum += element;
            }
            double mean = sum / combinedPaneElements.size();
            out.collect(mean);
        }
    }

    private List<Long> getWindowPaneIds(TimeWindow window) {
        long windowStart = window.getStart();
        long windowEnd = window.getEnd();
        long paneStart = windowEnd - paneSize;
        List<Long> paneIds = new ArrayList<>();
        while (paneStart >= windowStart) {
            paneIds.add(getPaneId(paneStart));
            paneStart -= slide;
        }
        return paneIds;
    }
    private long gcd(long a, long b) {
        if (b == 0) {
            return a;
        } else {
            return gcd(b, a % b);
        }
    }
}
