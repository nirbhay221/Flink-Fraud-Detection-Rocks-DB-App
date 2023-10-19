package InorderTumblingWithParallelism;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Status;
import org.rocksdb.util.SizeUnit;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static utils.AppBase.pathToRocksDB3;

public class RocksDBManager implements Serializable {

    private final Map<Integer, RocksDB> rocksDBInstances = new HashMap<>();

    public RocksDBManager(int numSubtasks) {
        for (int i = 0; i < numSubtasks; i++) {
            try {
                Options options = new Options()
                        .setCreateIfMissing(true)
                        .setIncreaseParallelism(2); // Set the parallelism level for RocksDB to 2

                RocksDB rocksDB = RocksDB.open(options, pathToRocksDB3 + i);
                rocksDBInstances.put(i, rocksDB);
            } catch (RocksDBException e) {
                throw new RuntimeException("Error initializing RocksDB instance for subtask " + i, e);
            }
        }
    }

    public RocksDB getOrCreateRocksDBInstance(int keyGroup) throws RocksDBException {
        if (!rocksDBInstances.containsKey(keyGroup)) {
            Options options = new Options()
                    .setCreateIfMissing(true)
                    .setIncreaseParallelism(2); // Set the parallelism level for RocksDB to 2

            try {
                RocksDB rocksDB = RocksDB.open(options, pathToRocksDB3 + keyGroup);
                rocksDBInstances.put(keyGroup, rocksDB);
            } catch (RocksDBException e) {
                // If the database is already locked, try opening a new instance with a different path
                if (e.getStatus().getCode() == Status.Code.IOError && e.getMessage().contains("lock hold by current process")) {
                    RocksDB rocksDB = RocksDB.open(options, pathToRocksDB3 + keyGroup + "_new");
                    rocksDBInstances.put(keyGroup, rocksDB);
                } else {
                    // Rethrow the exception if it's not related to a locked database
                    throw e;
                }
            }
        }
        return rocksDBInstances.get(keyGroup);
    }



    public void closeAll() throws RocksDBException {
        for (RocksDB rocksDB : rocksDBInstances.values()) {
            rocksDB.close();
        }
    }
}
