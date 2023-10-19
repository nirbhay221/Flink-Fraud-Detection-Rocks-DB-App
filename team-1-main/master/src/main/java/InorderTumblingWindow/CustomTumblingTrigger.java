package InorderTumblingWindow;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.nio.charset.StandardCharsets;

import static utils.AppBase.pathToRocksDB;

public class CustomTumblingTrigger extends Trigger<Tuple3<Long, Double, Long>, TimeWindow> {

    private long windowSize;
    private transient RocksDB rocksDB;

    public CustomTumblingTrigger(long windowSize) {
        this.windowSize = windowSize;
    }

    @Override
    public TriggerResult onElement(Tuple3<Long, Double, Long> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // Register an event time timer
        ctx.registerEventTimeTimer(window.maxTimestamp());
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("On Event Time");
        // Check if the watermark has passed the end of the window
        if (ctx.getCurrentWatermark() >= window.getEnd()) {
            // Open RocksDB instance
            synchronized (FraudDetectionJob.rocksDBLock) {
                if (rocksDB == null) {
                    synchronized (FraudDetectionJob.rocksDBLock) {

                        String dbPath = pathToRocksDB;
                        File dbDir = new File(dbPath);
                        if (!dbDir.exists()) {
                            if (!dbDir.mkdirs()) {
                                throw new RuntimeException("Failed to create RocksDB directory: " + dbPath);
                            }
                        }

                        rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), dbPath);
//                        rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), "");
                    }     }
            }

            // Scan the buffer (RocksDB) and retrieve all records whose timestamp falls inside the window bounds
            RocksIterator iterator = rocksDB.newIterator();
            for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                String[] keyParts = new String(iterator.key()).split("_");
                long recordTimestamp = Long.parseLong(keyParts[0]);
                long accountId = Long.parseLong(keyParts[1]);

                if (recordTimestamp >= window.getStart() && recordTimestamp < window.getEnd()) {
                    // Get the value from RocksDB and display the account id
                    byte[] valueBytes;
                    synchronized (FraudDetectionJob.rocksDBLock) {
                        valueBytes = rocksDB.get(iterator.key());
                    }
                    if (valueBytes != null) {
                        String valueString = new String(valueBytes, StandardCharsets.UTF_8);
                        System.out.println("Account ID: " + accountId + ", Value: " + valueString);
                    }
                } else if (recordTimestamp >= window.getEnd()) {
                    break; // Records are ordered by timestamp, no need to check further
                }
            }

            // Close RocksDB instance
            synchronized (FraudDetectionJob.rocksDBLock) {
                rocksDB.close();
                rocksDB = null;
            }

            // Clear the buffer and return FIRE_AND_PURGE
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // The watermark has not passed the end of the window yet, so continue buffering elements
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        System.out.println("On Processing Time");
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // Clear the timer state
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        long windowMaxTimestamp = window.maxTimestamp();
        if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
            ctx.registerEventTimeTimer(windowMaxTimestamp);
        }
    }
}
