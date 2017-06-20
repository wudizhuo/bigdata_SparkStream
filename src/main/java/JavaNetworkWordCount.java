import org.apache.spark.SparkConf;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.regex.Pattern;

public class JavaNetworkWordCount {
    public static String joinStr = "~~~";
    public static final Pattern SPACE = Pattern.compile(joinStr);
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
            System.exit(1);
        }

        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverFlow");
        final JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                new CustomReceiver(StorageLevels.MEMORY_AND_DISK_SER));
        JavaDStream<String> map = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) throws Exception {
                String[] split = SPACE.split(s);
                return !split[split.length - 2].equals("NA");
            }
        }).map(new Function<String, String>() {
            public String call(String s) throws Exception {
                String[] split = SPACE.split(s);
                return split[13] + "   " + split[split.length - 2];
            }
        });
        map.print();
//        map.dstream().saveAsTextFiles("file:///home/cloudera/Downloads/Project/", "StackOverFlow");
        map.dstream().saveAsTextFiles("hdfs://quickstart:8020/user/cloudera/output/", "StackOverFlow");
        ssc.start();
        ssc.awaitTermination();
    }
}