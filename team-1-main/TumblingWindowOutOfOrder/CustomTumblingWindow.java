package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.IOException;
import java.util.*;
public class CustomTumblingWindow extends WindowAssigner<Tuple2<Long, Double>, TimeWindow> {

    private final long size;
    private final long maxWindowsPerAssign;
    private final Map<Long, List<Tuple2<Long, Double>>> buffer;
    private transient RocksDB rocksDB;
    private int windowCount;

    public CustomTumblingWindow(long size, long maxWindowsPerAssign) {
        this.size = size;
        this.maxWindowsPerAssign = maxWindowsPerAssign;
        this.buffer = new HashMap<>();
        this.windowCount = 0;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Tuple2<Long, Double> element, long timestamp, WindowAssignerContext context) {
        long accountId = element.f0;
        double value = element.f1;

        // Add the element to the buffer for the appropriate account ID
        List<Tuple2<Long, Double>> accountBuffer = buffer.computeIfAbsent(accountId, k -> new ArrayList<>());
        accountBuffer.add(element);

        // Check if any windows can be triggered for the account
        List<TimeWindow> windows = new ArrayList<>();
        long currentTime = context.getCurrentProcessingTime();
        while (!accountBuffer.isEmpty() && accountBuffer.get(0).f0 + size <= currentTime) {
            // Retrieve all records for the account whose timestamp falls inside the window bounds
            List<Tuple2<Long, Double>> windowElements = new ArrayList<>();
            int i = 0;
            while (i < accountBuffer.size() && accountBuffer.get(i).f0 < currentTime) {
                if (accountBuffer.get(i).f0 + size <= currentTime) {
                    windowElements.add(accountBuffer.get(i));
                }
                i++;
            }

            // Create and add the window
            long windowStart = currentTime - (currentTime % size);
            long windowEnd = windowStart + size;
            windows.add(new TimeWindow(windowStart, windowEnd));

            // Remove the processed elements for the account from the buffer
            accountBuffer.subList(0, i).clear();

            // Process the elements for the account and update the record and window count
            for (Tuple2<Long, Double> windowElement : windowElements) {
                // Get the account id and value from the incoming element
                accountId = windowElement.f0;
                value = windowElement.f1;

                // Open RocksDB and insert the account id and value
                try {
                    // Get or open the RocksDB instance
                    if (rocksDB == null) {
                        rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), "/home/seednew/Downloads/workspace/frauddetection/rocksdb_data");
                    }

                    // Put the account id and value into RocksDB under the appropriate ID
                    rocksDB.put(Long.toString(accountId).getBytes(), Double.toString(value).getBytes());

                    // Update the record and window count
                    windowCount++;
                    if (windowCount >= maxWindowsPerAssign) {
                        closeRocksDB();
                    }
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        return windows;
    }
    private void closeRocksDB() throws RocksDBException {
        if (rocksDB != null) {
            rocksDB.close();
            rocksDB = null;
            windowCount = 0;
        }
    }


    @Override
    public Trigger<Tuple2<Long, Double>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return new CustomTumblingTrigger(3000);
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
        return "CustomTumblingWindow(" + size + ")";
    }
}
