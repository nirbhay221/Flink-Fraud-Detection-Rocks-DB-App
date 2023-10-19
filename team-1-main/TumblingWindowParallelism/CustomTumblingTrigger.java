package spendreport;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
public class CustomTumblingTrigger extends Trigger<Tuple2<Long, Double>, TimeWindow> {

    private long windowSize;
    private transient RocksDB rocksDB;
    private long maxWatermark = Long.MIN_VALUE;
    private int subtaskIndex;

    private final RocksDBManager rocksDBManager;

    public CustomTumblingTrigger(long windowSize, int subtaskIndex) {
        this.windowSize = windowSize;
        this.subtaskIndex = subtaskIndex;

        this.rocksDBManager = new RocksDBManager(subtaskIndex);

    }

    // Method to initialize RocksDB with the given options
    private void initRocksDB() throws RocksDBException {
        Options options = new Options()
                .setCreateIfMissing(true)
                .setIncreaseParallelism(2); // Set the parallelism level for RocksDB to 2

        // Initialize RocksDB
        rocksDB = RocksDB.open(options, "/home/seednew/Downloads/workspace/frauddetection/rocksdb_data_" + subtaskIndex);
    }

    @Override
    public synchronized TriggerResult onElement(Tuple2<Long, Double> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
        // Update the watermark
        maxWatermark = Math.max(maxWatermark, element.f0);

        // Get the RocksDB instance for the appropriate key group
        long accountId = element.f0;
        int keyGroup = KeyGroupRangeAssignment.assignKeyToParallelOperator(accountId, 2, 0);
        RocksDB rocksDBInstance = rocksDBManager.getOrCreateRocksDBInstance(keyGroup);

        // Store the element in RocksDB
        byte[] keyBytes = (element.f0 + ":" + element.f1).getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = serializeObject(element);
        rocksDBInstance.put(keyBytes, valueBytes);

        return TriggerResult.CONTINUE;
    }

    @Override
    public synchronized TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // Check if the watermark has passed the end of the window
        if (maxWatermark >= window.getEnd()) {
            // Retrieve all records within the window bounds
            byte[] startKey = (window.getStart() + ":").getBytes(StandardCharsets.UTF_8);
            byte[] endKey = (window.getEnd() + ":").getBytes(StandardCharsets.UTF_8);
            List<Tuple2<Long, Double>> output = new ArrayList<>();
            if (rocksDB != null) {
                try (RocksIterator iterator = rocksDB.newIterator()) {
                    for (iterator.seek(startKey); iterator.isValid() && Arrays.compare(iterator.key(), endKey) < 0; iterator.next()) {
                        // Retrieve the value for the key and output it
                        byte[] valBytes = rocksDB.get(iterator.key());
                        if (valBytes != null) {
                            Tuple2<Long, Double> value = (Tuple2<Long, Double>) deserializeObject(valBytes);
                            output.add(value);
                        }
                    }
                }catch (RocksDBException e) {
                    throw new RuntimeException(e);
                } finally {
                    if (rocksDB != null) {
                        rocksDBManager.closeAll();
                    }
                }

            }

            // Clear the buffer and return FIRE_AND_PURGE
            return TriggerResult.FIRE_AND_PURGE;
        } else {
            // Register an event time timer
            ctx.registerEventTimeTimer(window.maxTimestamp());
            // The watermark has not passed the end of the window yet, so continue buffering elements
            return TriggerResult.CONTINUE;
        }
    }

    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
        // Retrieve all records within the window bounds
        byte[] startKey = (window.getStart() + ":").getBytes(StandardCharsets.UTF_8);
        byte[] endKey = (window.getEnd() + ":").getBytes(StandardCharsets.UTF_8);
        List<Tuple2<Long, Double>> output = new ArrayList<>();
        if (rocksDB != null) {
            try (RocksIterator iterator = rocksDB.newIterator()) {
                for (iterator.seek(startKey); iterator.isValid() && Arrays.compare(iterator.key(), endKey) < 0; iterator.next()) {
                    // Retrieve the value for the key and output it
                    byte[] valBytes = rocksDB.get(iterator.key());
                    if (valBytes != null) {
                        Tuple2<Long, Double> value = (Tuple2<Long, Double>) deserializeObject(valBytes);
                        output.add(value);
                    }
                }
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            } finally {
                if (rocksDB != null) {
                    rocksDBManager.closeAll();
                }
            }


        }

        // Clear the buffer
        return TriggerResult.FIRE_AND_PURGE;
    }



    @Override
    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
        // Clear the timer state
        ctx.deleteEventTimeTimer(window.maxTimestamp());
    }

    @Override
    public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(new ListStateDescriptor<>("output", TypeInformation.of(new TypeHint<Object>() {})));
    }


    public void close() throws Exception {
        if (rocksDB != null) {
            rocksDB.close();
        }
    }

    private static byte[] serializeObject(Object object) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(object);
            return bos.toByteArray();
        }

}
    private static Object deserializeObject(byte[] bytes) throws IOException, ClassNotFoundException {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            return ois.readObject();
        }
    }
}
