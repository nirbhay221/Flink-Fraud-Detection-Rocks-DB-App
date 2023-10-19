package CustomWindowAssigner;

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


import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
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
//                        rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), "C:/Users/yuesiliu/Desktop/abc/team-1/master/rocksdb_data");
                        String dbPath = "file:///home/ram01/SS/t5/rocksdb_data";
                        File dbDir = new File(dbPath);
                        if (!dbDir.exists()) {
                            if (!dbDir.mkdirs()) {
                                throw new RuntimeException("Failed to create RocksDB directory: " + dbPath);
                            }
                        }
                        Options options = new Options();
                        //options.setCreateIfMissing(true);
                        //options.setUseAdaptiveMutex(false);
                        //options.setMaxTotalWalSize(256 * 1024 * 1024);
                        //options.max_open_files(1000);
                        options.setIncreaseParallelism(4);
                        options.setMaxBackgroundCompactions(4);

                        options.setUseAdaptiveMutex(false);
                        options.setWriteBufferSize(67108864);

                        options.setMaxBytesForLevelBase(268435456);
                        options.setTargetFileSizeBase(67108864);
                        options.setMaxTotalWalSize(536870912);
                        options.setDbWriteBufferSize(67108864);
                        options.setAllowConcurrentMemtableWrite(true);


 //                       options.setMemtablePrefixBloomHugePageTlbSize(1000);
 //                       options.setTableFormatConfig(new BlockBasedTableConfig().setBlockSize(64 * 1024).setBlockCacheSize(64 * 1024 * 1024).setFilter(new BloomFilter(10, false)).setCacheIndexAndFilterBlocks(true).setPinL0FilterAndIndexBlocksInCache(true).setFormatVersion(2).setBlockRestartInterval(16).setBlockSizeDeviation(5).setBlockCacheCompressedSize(64 * 1024 * 1024).setCacheNumShardBits(6).setIndexType(IndexType.kHashSearch).setHashIndexAllowCollision(false));
                        options.setUseFsync(false);
                        options.setUseAdaptiveMutex(false);
                        options.setUseDirectReads(false);
                        options.setUseDirectIoForFlushAndCompaction(false);
                        options.setIncreaseParallelism(4);
                        options.setMaxBackgroundCompactions(4);
   //                     options.setCompactionStyle(CompactionStyle.UNIVERSAL);
                        options.setUseAdaptiveMutex(false);
                        options.setWriteBufferSize(67108864);
     //                   options.setOptimizeLevelStyleCompaction(0);
                        options.setMaxBytesForLevelBase(268435456);
                        options.setTargetFileSizeBase(67108864);
                        options.setMaxTotalWalSize(536870912);
                        options.setDbWriteBufferSize(67108864);
                        options.setAllowConcurrentMemtableWrite(true);
      //                  options.setMemtablePrefixBloomBits(1024 * 1024 * 64);
      //                  options.setMemtablePrefixBloomProbes(6);
        //                options.setMemtablePrefixBloomHugePageTlbSize(1000);
          //              options.setTableFormatConfig(new BlockBasedTableConfig().setBlockSize(64 * 1024).setBlockCacheSize(64 * 1024 * 1024).setFilter(new BloomFilter(10, false)).setCacheIndexAndFilterBlocks(true).setPinL0FilterAndIndexBlocksInCache(true).setFormatVersion(2).setBlockRestartInterval(16).setBlockSizeDeviation(5).setBlockCacheCompressedSize(64 * 1024 * 1024).setCacheNumShardBits(6).setIndexType(IndexType.kHashSearch).setHashIndexAllowCollision(false));
                        options.setUseFsync(false);
                        options.setUseAdaptiveMutex(false);
                        options.setUseDirectReads(false);
                        options.setUseDirectIoForFlushAndCompaction(false);
                        options.setIncreaseParallelism(4);
                        options.setMaxBackgroundCompactions(4);
       //                 options.setCompactionStyle(CompactionStyle.UNIVERSAL);
                        options.setUseAdaptiveMutex(false);

                        //options.setMaxTotalWalSize(256 * 1024 * 1024);
                        //rocksDB = RocksDB.open(new Options().setCreateIfMissing(true), dbPath);
                        rocksDB = RocksDB.open(options, dbPath);

                    }

                    // Put the account id and value into RocksDB under the appropriate ID
                    rocksDB.put(Long.toString(accountId).getBytes(), Double.toString(value).getBytes());

                    //Printing of the values inside the window
                    byte[] key = Long.toString(accountId).getBytes();
                    byte[] fetchedValue = rocksDB.get(key);
                    //fix the value for the storage
                    System.out.println("the key after the storage is: " + new BigInteger(1, key).longValue() + " the value is: " + Double.parseDouble(new String(fetchedValue)));
                    System.out.println("the key before the storage is: " + accountId+ " the value is: " + value);

                    // Update the record and window count
                    windowCount++;
                    if (windowCount >= maxWindowsPerAssign) {
                        closeRocksDB();
//                        break;
                    }
                } catch (RocksDBException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    if (rocksDB != null){
                        try {
                            closeRocksDB();
                        } catch (RocksDBException e) {
                            throw new RuntimeException(e);
                        }
                    }
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
        return false;
    }

    @Override
    public String toString() {
        return "CustomTumblingWindow(" + size + ")";
    }
}



