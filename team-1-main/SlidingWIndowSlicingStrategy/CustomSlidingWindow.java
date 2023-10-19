package spendreport;

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

import java.util.*;public class CustomSlidingWindow extends WindowAssigner<Tuple2<Long, Double>, TimeWindow> {

    public final long size;
    private final long slide;
    private final long paneSize;
    public final Map<Long, Double> paneSumMap;
    public final Map<Long, Integer> paneCountMap;
    public final Map<Long, Double> paneMeanMap;
    public final Map<Long, Long> timestampToPaneIdMap;

    public CustomSlidingWindow(long size, long slide) {
        this.size = size;
        this.slide = slide;
        this.paneSize = gcd(size, slide);
        this.paneSumMap = new HashMap<>();
        this.paneCountMap = new HashMap<>();
        this.paneMeanMap = new HashMap<>();
        this.timestampToPaneIdMap = new HashMap<>();
    }

    @Override
    public Collection<TimeWindow> assignWindows(Tuple2<Long, Double> element, long timestamp, WindowAssignerContext context) {
        long paneId = getPaneId(timestamp);
        long elementEnd = (paneId + 1) * paneSize;
        long paneStart = elementEnd - paneSize;
        TimeWindow window = new TimeWindow(paneStart, elementEnd);
        addToPane(element, paneId);
        timestampToPaneIdMap.put(timestamp, paneId);
        return Collections.singletonList(window);
    }

    @Override
    public Trigger<Tuple2<Long, Double>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment streamExecutionEnvironment) {
        return new CustomTrigger(size / paneSize, this);
    }

    public long getPaneId(long timestamp) {
        if (paneSize == 0) {
            // Handle the case where paneSize is 0
            // You can throw an exception, log an error, or return a default value, depending on your requirements
            throw new ArithmeticException("paneSize cannot be 0");
        }
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

    private void addToPane(Tuple2<Long, Double> element, long paneId) {
        double value = element.f1;
        double sum = paneSumMap.getOrDefault(paneId, 0.0) + value;
        int count = paneCountMap.getOrDefault(paneId, 0) + 1;
        double mean = sum / count;
        paneSumMap.put(paneId, sum);
        paneCountMap.put(paneId, count);
        paneMeanMap.put(paneId, mean);
    }
    public List<Long> getWindowPaneIds(TimeWindow window) {
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

