package utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class AppBase {
    public static SourceFunction<String> strings = null;
    public static SinkFunction out = null;
    public static int parallelism = 1;

    public final static String pathToRocksDB = "C:/Users/yuesiliu/Desktop/Reorder/team-1/master/rocksDB_data";
    public final static String pathToRocksDB2 = "C:/Users/yuesiliu/Desktop/just for git push/team-1/master/rocksdb_data";
    public final static String pathToRocksDB3 = "C:/Users/yuesiliu/Desktop/Reorder/team-1/master/rocksDB_data_";
    public final static String pathToRocksDBCheckpoint = "file:///C:/Users/yuesiliu/Desktop/just for git push/team-1/master/check_point";
    public final static String pathToRocksDBCheckpoint2 = "file:///C:/Users/yuesiliu/Desktop/Reorder/team-1/master/rocksDB_data";
    public static SinkFunction<?> sinkOrTest(SinkFunction<?> sink) {
        if (out == null) {
            return sink;
        }
        return out;
    }

    public static SourceFunction<String> stringSourceOrTest(SourceFunction<String> source) {
        if (strings == null) {
            return source;
        }
        return strings;
    }

    public static void printOrTest(org.apache.flink.streaming.api.datastream.DataStream<?> ds) {
        if (out == null) {
            ds.print();
        } else {
            ds.addSink(out);
        }
    }
}
//Paths:
// "file:///home/user/Desktop/Stream-CS551/project/team-1/master/rocksdb_data"
// "C:/Users/yuesiliu/Desktop/Reorder/team-1/master/rocksDB_data"
// "file:///C:/Users/yuesiliu/Desktop/just for git push/team-1/master/check_point"