package process;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import process.entity.IDRecord;
import process.entity.SimpleRecord;
import process.entity.User;
import process.entity.Video;
import process.utils.FileUtil;
import scala.Tuple2;

// 生成 id_record.txt，中间结果
// userid,videoid,watchtime,ht
//
// spark-submit --driver-memory 128G --executor-memory 128G --conf spark.driver.maxResultSize=128g  --master localhost --class "process.ConvertToIDRecord"  data_process-1.0-SNAPSHOT.jar
public class ConvertToIDRecord {

    private final static String user_file = "data/dataprocess/original/users.txt";
    private final static String video_file = "data/dataprocess/original/videos.txt";
    private final static String record_file = "data/dataprocess/original/simple_record.txt";
    private final static String id_record_save = "data/dataprocess/original/id_record.txt";

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("org");
        logger.setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("app_cs")
                .setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        System.out.println("hello cs");


        //加载数据
        JavaRDD<User> users = jsc.textFile(user_file).map(line -> new User(line));
        JavaRDD<Video> videos = jsc.textFile(video_file).map(line -> new Video(line));
        JavaRDD<SimpleRecord> simpleRecords = jsc.textFile(record_file).map(line -> new SimpleRecord(line));
        System.out.println("read");

        //转为id
        JavaPairRDD<String, User> userpair = users.mapToPair(user -> new Tuple2<>(user.rowkey, user));
        JavaPairRDD<String, Video> videopair = videos.mapToPair(video -> new Tuple2<>(video.videoname, video));
        System.out.println("maptopair");

//        userpair.collect().forEach(s -> System.out.println(s));
//        videopair.collect().forEach(s -> System.out.println(s));

        //转为id
        JavaRDD<IDRecord> idRecordJavaRDD = simpleRecords
                .mapToPair(simpleRecord -> new Tuple2<>(simpleRecord.rowkey, simpleRecord))
                .join(userpair) // rowkey -> userid
                .mapToPair(tuple -> new Tuple2<>(
                        tuple._2._1.videoname,
                        new IDRecord(tuple._2._2.userid, null, tuple._2._1.watchtime, tuple._2._1.ht)
                ))
                .join(videopair)    // videoname -> videoid
                .values()
                .map(tuple -> {
                    IDRecord idRecord = tuple._1;
                    idRecord.videoid = tuple._2.videoid;
                    return idRecord;
                });

        FileUtil.write(idRecordJavaRDD.map(s -> s.toWriteLine()).collect(), id_record_save);

        jsc.close();
    }


}
