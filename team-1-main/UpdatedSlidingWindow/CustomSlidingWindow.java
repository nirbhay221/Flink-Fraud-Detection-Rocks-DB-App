package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.*;

public class CustomSlidingWindow extends WindowAssigner<Tuple3<Long, Double, Long>, TimeWindow> {

    public final long size;
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
    public Collection<TimeWindow> assignWindows(Tuple3<Long, Double, Long> element, long timestamp, WindowAssignerContext context) {
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
    public Trigger<Tuple3<Long, Double, Long>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
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

    private void addToPane(Tuple3<Long, Double, Long> element, long timestamp, long paneId) {
        paneElementsMap.computeIfAbsent(paneId, k -> new ArrayList<>()).add(element.f1);
    }
    public Tuple2<Long, Double> combinePanes(TimeWindow window) {
        List<Long> paneIds = getWindowPaneIds(window);
        List<Double> combinedPaneElements = new ArrayList<>();
        Long accountId = null;
        for (long paneId : paneIds) {
            List<Double> paneElements = paneElementsMap.get(paneId);
            if (paneElements != null) {
                combinedPaneElements.addAll(paneElements);
                if (accountId == null) {
                    accountId = paneElementsMap.keySet().stream()
                            .filter(k -> !paneElementsMap.get(k).isEmpty())
                            .findFirst()
                            .orElse(null);
                }
            }
        }
        if (!combinedPaneElements.isEmpty()) {
            double sum = 0.0;
            for (Double element : combinedPaneElements) {
                sum += element;
            }
            double mean = sum / combinedPaneElements.size();
            return new Tuple2<>(accountId, mean);
        } else {
            return null;
        }
    }


    private long getWindowStartTimestamp(long paneId) {
        return paneId * paneSize;
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
    }    private long gcd(long a, long b) {
        if (b == 0) {
            return a;
        } else {
            return gcd(b, a % b);
        }
    }
}

