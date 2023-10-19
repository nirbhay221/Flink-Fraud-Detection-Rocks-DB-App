package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;
public class CustomTumblingWindow extends WindowAssigner<Tuple2<Long, Double>, TimeWindow> {

    private final long size;
    private final long maxWindowsPerAssign;
    private final Map<Long, List<Tuple2<Long, Double>>> buffer;
    private transient RocksDB rocksDB;

    private int windowCount;

    private final Map<Integer, RocksDB> rocksDBInstances;
    private final int subtaskIndex;
    private final RocksDBManager rocksDBManager;

    public CustomTumblingWindow(long size, long maxWindowsPerAssign, int subtaskIndex) {
        this.size = size;
        this.maxWindowsPerAssign = maxWindowsPerAssign;
        this.buffer = new HashMap<>();
        this.windowCount = 0;
        this.subtaskIndex = subtaskIndex;
        this.rocksDBInstances = new HashMap<>();
        this.rocksDBManager = new RocksDBManager(subtaskIndex);

    }

    @Override
    public synchronized Collection<TimeWindow> assignWindows(Tuple2<Long, Double> element, long timestamp, WindowAssignerContext context) {
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

            for (Tuple2<Long, Double> windowElement : windowElements) {
                // Get the account id and value from the incoming element
                accountId = windowElement.f0;
                value = windowElement.f1;
                // Open RocksDB and insert the account id and value
                try {
                    // Get or open the RocksDB instance
                    int keyGroup = KeyGroupRangeAssignment.assignKeyToParallelOperator(accountId, 2, 0);
                    RocksDB rocksDBInstance = rocksDBManager.getOrCreateRocksDBInstance(keyGroup);

                    rocksDBInstance.put(Long.toString(accountId).getBytes(), Double.toString(value).getBytes());

                    // Update the record and window count
                    windowCount++;
                    if (windowCount >= maxWindowsPerAssign) {
                       rocksDBManager.closeAll();
                    }
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                } finally {
                }

        }


    }

        return windows;
    }
    private RocksDB getOrCreateRocksDBInstance(int subtaskIndex, int keyGroup) throws RocksDBException {
        if (!rocksDBInstances.containsKey(keyGroup)) {
            try (Options options = new Options().setCreateIfMissing(true).setIncreaseParallelism(2)) {
                String dbPath = "/home/seednew/Downloads/workspace/frauddetection/rocksdb_data_" + subtaskIndex + "_" + keyGroup;
                RocksDB rocksDBInstance = RocksDB.open(options, dbPath);
                rocksDBInstances.put(keyGroup, rocksDBInstance);
            }
        }
        return rocksDBInstances.get(keyGroup);
    }

    private void closeAllRocksDBInstances() throws RocksDBException {
        for (RocksDB rocksDBInstance : rocksDBInstances.values()) {
            if (rocksDBInstance != null) {
                rocksDBInstance.close();
            }
        }
        rocksDBInstances.clear();
        windowCount = 0;
    }


    @Override
    public Trigger<Tuple2<Long, Double>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
        return null;
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
    }
