package spendreport;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.util.*;
public class CustomTumblingWindow extends WindowAssigner<Tuple3<Long, Double, Long>, TimeWindow> {

    private final long size;
    private final long maxWindowsPerAssign;
    private transient RocksDB rocksDB;
    private int windowCount;
    private transient RocksDBManager rocksDBManager;
    private int keyGroup;

    public CustomTumblingWindow(long size, long maxWindowsPerAssign) {
        this.size = size;
        this.maxWindowsPerAssign = maxWindowsPerAssign;
        this.windowCount = 0;
    }

    @Override
    public Collection<TimeWindow> assignWindows(Tuple3<Long, Double, Long> element, long timestamp, WindowAssignerContext context) {
        long accountId = element.f0;
        double value = element.f1;
        long elementTimestamp = element.f2;

        // Create and add the window
        long windowStart = elementTimestamp - (elementTimestamp % size);
        long windowEnd = windowStart + size;
        TimeWindow window = new TimeWindow(windowStart, windowEnd);

        // Store the element in RocksDB with a composite key based on the timestamp and account id
        String compositeKey = elementTimestamp + "_" + accountId;

        // Update keyGroup based on accountId
        keyGroup = (int) (accountId % 2); // Assuming parallelism of 2

        try {
            // Get or open the RocksDB instance
            synchronized (FraudDetectionJob.rocksDBLock) {
                if (rocksDBManager == null) {
                    synchronized (FraudDetectionJob.rocksDBLock) {
                        rocksDBManager = new RocksDBManager(2); // Assuming parallelism of 2
                    }
                }
                rocksDB = rocksDBManager.getOrCreateRocksDBInstance(keyGroup);
            }

            // Put the account id, value, and timestamp into RocksDB under the composite key
            synchronized (FraudDetectionJob.rocksDBLock) {
                rocksDB.put(compositeKey.getBytes(), Double.toString(value).getBytes());
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        try {
            closeRocksDB();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
        return Collections.singletonList(window);
    }

    private void closeRocksDB() throws RocksDBException {
        synchronized (FraudDetectionJob.rocksDBLock) {
            if (rocksDB != null) {
                rocksDB.close();
                rocksDB = null;
                windowCount = 0;
            }
        }
    }

    @Override
    public Trigger<Tuple3<Long, Double, Long>, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
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
