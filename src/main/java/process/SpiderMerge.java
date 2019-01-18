package process;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import process.entity.DoubanVideo;
import process.entity.Video;
import process.utils.Cleaner;
import process.utils.FileUtil;
import process.utils.SpiderMatcher;

import java.util.List;

// 用爬虫数据填补videos.txt
public class SpiderMerge {

    private final static String video_file = "data/dataprocess/original/videos.txt";
    private final static String douban_file = "data/dataprocess/original/pachong_merge_final.txt";

    private static JavaRDD<Video> videos;

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("org");
        logger.setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName("app_cs")
                .setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);


        videos = jsc.textFile(video_file)
                .map(line -> new Video(line));
        List<Video> videoList = videos.collect();
        //videos.collect().forEach(s -> System.out.println(s));

        JavaRDD<String> doubanlines = jsc.textFile(douban_file);
        JavaRDD<DoubanVideo> doubans = Cleaner.doubanCleaner(doubanlines);
        //doubans.collect().forEach(s -> System.out.println(s));

        //遍历豆瓣节目
        int matchcount = 0;
        for (DoubanVideo doubanVideo : doubans.collect()) {
            for (Video video : videoList) {
                if (SpiderMatcher.isMatch(doubanVideo, video)) {
                    matchcount++;
                    System.out.println("matchcount=" + matchcount + ": " + video.videoname + " <-> " + doubanVideo.names);
                    //填充字段
                    if (video.category == null) {
                        video.category = doubanVideo.category;
                        System.out.println("aaa");
                    }
                    if (video.videotimelength == 0.0 && doubanVideo.videotimelength != null) {
                        video.videotimelength = doubanVideo.videotimelength;
                        System.out.println("bbb");
                    }
                    video.directs.addAll(doubanVideo.directs);
                    video.actors.addAll(doubanVideo.actors);
                    video.plots.addAll(doubanVideo.plots);
                    video.regions.addAll(doubanVideo.regions);
                }
            }
        }

        FileUtil.write(videos.map(v -> v.toSimpleString()).collect(), video_file);


        jsc.close();
    }


}
