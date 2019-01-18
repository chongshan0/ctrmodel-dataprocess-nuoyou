package process;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import process.entity.IDRecord;
import process.entity.Score;
import process.entity.User;
import process.entity.Video;
import process.utils.FileUtil;
import scala.Tuple2;

//生成score_all.txt 和 score_adult.txt，会用于构造排序模型的输入特征
// userid;videoid;watchtimesum;percentage;rui;watchcountsum
//
// spark-submit --driver-memory 128G --executor-memory 128G --conf spark.driver.maxResultSize=128g  --master localhost --class "process.GenerateScore"  data_process-1.0-SNAPSHOT.jar
public class GenerateScore {

    private final static String user_file = "data/dataprocess/original/users.txt";
    private final static String video_file = "data/dataprocess/original/videos.txt";
    private final static String id_record_save = "data/dataprocess/original/id_record.txt";

    private final static String score_all_save = "data/dataprocess/result/score_all.txt";
    private final static String score_adult_save = "data/dataprocess/result/score_adult.txt";

    private static JavaRDD<IDRecord> idRecordJavaRDD;
    private static JavaRDD<User> users;
    private static JavaRDD<Video> videos;
    private static JavaPairRDD<Long, User> userpair;
    private static JavaPairRDD<Long, Video> videopair;

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("org");
        logger.setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("app_cs").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        System.out.println("hello cs");

        //加载数据
        System.out.println("read");
        idRecordJavaRDD = jsc.textFile(id_record_save)
                .filter(line -> line.split(",").length == 4)
                .map(line -> new IDRecord(line));

        users = jsc.textFile(user_file).map(line -> new User(line));
        videos = jsc.textFile(video_file).map(line -> new Video(line));

        System.out.println("maptopair");
        userpair = users.mapToPair(user -> new Tuple2<>(user.userid, user));
        videopair = videos.mapToPair(video -> new Tuple2<>(video.videoid, video));

        userpair.cache();
        videopair.cache();

        //idRecordJavaRDD.collect().forEach(s -> System.out.println(s));

        //打分
        JavaRDD<process.entity.Score> scores_all = generate_score_all();
        JavaRDD<process.entity.Score> scores_adult = generate_score_adult(scores_all);

        scores_all.cache();
        scores_adult.cache();

        FileUtil.write(scores_all.map(s -> s.toWriteLine()).collect(), score_all_save);
        FileUtil.write(scores_adult.map(s -> s.toWriteLine()).collect(), score_adult_save);

        System.out.println(scores_all.count());
        System.out.println(scores_adult.count());

        jsc.close();
    }

    //生成打分（不分成人儿童）
    private static JavaRDD<process.entity.Score> generate_score_all() {
        //获取每个u-v对的观看时长总长
        JavaRDD<process.entity.Score> watchtimeSum = idRecordJavaRDD
                .mapToPair(idRecord -> new Tuple2<>(
                        idRecord.userid + "#" + idRecord.videoid,   //key
                        idRecord
                ))
                .mapValues(idRecord -> new Tuple2<>(idRecord, new Long(1)))
                .reduceByKey((t1, t2) -> {    //聚合
                    IDRecord idRecord = t1._1;
                    idRecord.watchtime = t1._1.watchtime + t2._1.watchtime; //时长sum
                    return new Tuple2<>(idRecord, t1._2 + t2._2);   //观看次数sum
                })
                .values()
                .map(t -> new Score(
                        t._1.userid,
                        t._1.videoid,
                        t._1.watchtime / 60000.0,
                        0.0,
                        0.0,
                        t._2));
        //watchtimeSum.collect().forEach(s -> System.out.println(s));

        //获取总观看时长占比
        JavaRDD<process.entity.Score> percentage = watchtimeSum
                .mapToPair(score -> new Tuple2<>(score.videoid, score)) //关联节目时长
                .join(videopair)
                .values()
                .map(t -> {
                    Score score = t._1;
                    score.percentage = score.watchtimesum / t._2.videotimelength;
                    return score;
                });

        // rui
        return percentage.map(score -> {
            score.rui = Math.log(1 + score.percentage);
            return score;
        });
    }

    //找出成人类的
    private static JavaRDD<process.entity.Score> generate_score_adult(JavaRDD<process.entity.Score> score_all) {
        return score_all
                .mapToPair(score -> new Tuple2<>(score.videoid, score))
                .join(videopair)
                .values()
                .filter(tuple -> {
                    //过滤儿童类节目
                    String category = tuple._2.category;
                    if (category.equals("动画") || category.equals("少儿") || category.equals("教育")) {
                        return false;
                    } else {
                        return true;
                    }
                })
                .filter(tuple -> {
                    //过滤未知类别节目
                    String category = tuple._2.category;
                    if (category.equals("")) {
                        return false;
                    } else {
                        return true;
                    }
                })
                .map(tuple -> tuple._1);
    }

}
