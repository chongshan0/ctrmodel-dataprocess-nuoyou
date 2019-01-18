package process;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.MinMaxScaler;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import process.entity.Feature;
import process.entity.Score;
import process.entity.Video;
import scala.Tuple2;

import java.util.HashMap;

//输入：score_adult.txt : userid;videoid;watchtimesum;percentage;rui;watchcountsum
//输出：features.txt : label;double[]
//输出用于构造排序模型特征
public class FeatureEngineer {

    private final static String video_file = "data/dataprocess/original/videos.txt";
    private final static String score_file = "data/dataprocess/result/score_adult.txt";
    private final static String feature_save_file = "data/dataprocess/result/features";

    private static JavaRDD<Score> scores;
    private static JavaRDD<Video> videos;
    private static JavaPairRDD<Long, Video> videopair;

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("org");
        logger.setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf().setAppName("app_cs").setMaster("local[4]");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);

        System.out.println("hello cs");

        //加载数据
        scores = jsc.textFile(score_file)
                .filter(line -> line.split(",").length == 6)
                .map(line -> new Score(line));
        videos = jsc.textFile(video_file).map(line -> new Video(line));
        videopair = videos.mapToPair(video -> new Tuple2<>(video.videoid, video));

        scores.cache();
        videopair.cache();

        //feature
        JavaRDD<Feature> feature1s = scores
                .mapToPair(score -> new Tuple2<>(score.videoid, score))
                .join(videopair)
                .values()
                .map(tuple2 -> {
                    Feature feature = new Feature();
                    feature.userid = tuple2._1.userid;
                    feature.videoid = tuple2._1.videoid;
                    feature.label = tuple2._1.percentage > 0.8 ? 1.0 : 0.0;
                    feature.secondlevel = tuple2._2.category;
                    feature.watchnum = tuple2._1.watchnumsum;
                    feature.watchperc = tuple2._1.percentage;
                    return feature;
                });

        //得到空的onehot
        HashMap<String, Integer> indexmap = new HashMap<>();
        int k = 0;
        for (Video video : videos.collect()) {
            if (!indexmap.containsKey(video.category))
                indexmap.put(video.category, k++);
            for (String string : video.plots) {
                if (!indexmap.containsKey(string))
                    indexmap.put(string, k++);
            }
            for (String string : video.regions) {
                if (!indexmap.containsKey(string))
                    indexmap.put(string, k++);
            }
        }
        //System.out.println(indexmap.size() + "xxx " + indexmap.values());

        //用户特征
        JavaPairRDD<Long, double[]> videofeatures = generate_video_embedding_all(indexmap);
        videofeatures.cache();

        //节目特征
        JavaPairRDD<Long, double[]> userfeatures = generate_user_embedding_all(videofeatures);
        scores.unpersist();

        //合并
        JavaRDD<Feature> feature2 = feature1s
                .mapToPair(feature -> new Tuple2<>(feature.videoid, feature))
                .join(videofeatures)
                .values()
                .map(tuple -> {
                    tuple._1.video_embedding = tuple._2;
                    return tuple._1;
                })
                .mapToPair(feature -> new Tuple2<>(feature.userid, feature))
                .join(userfeatures)
                .values()
                .map(tuple -> {
                    tuple._1.user_embedding = tuple._2;
                    return tuple._1;
                })
                .map(feature -> {
                    feature.user_e = new DenseVector(feature.user_embedding);
                    feature.video_e = new DenseVector(feature.video_embedding);
                    return feature;
                });

        videopair.unpersist();
        videofeatures.unpersist();
        userfeatures.unpersist();

        //转为df
        System.out.println("转为df");
        DataFrame dataFrame = sqlContext.createDataFrame(feature2, Feature.class);
//        dataFrame.show();
//        dataFrame.printSchema();

        //再次处理特征
        StringIndexer stringIndexer = new StringIndexer()   //secondlevel转为onehot
                .setInputCol("secondlevel")
                .setOutputCol("content_type_index");
        OneHotEncoder oneHotEncoder = new OneHotEncoder()   //secondlevel转为onehot
                .setInputCol("content_type_index")
                .setOutputCol("content_type_vector")
                .setDropLast(false);
        String[] vectorAsCols =
                {"content_type_vector", "watchnum", "watchperc", "user_e", "video_e"};
        VectorAssembler vectorAssembler = new VectorAssembler() //组合向量
                .setInputCols(vectorAsCols)
                .setOutputCol("vectorFeature");
        MinMaxScaler scaler = new MinMaxScaler()    //重新缩放
                .setInputCol("vectorFeature")
                .setOutputCol("scaledFeatures");

        PipelineStage[] pipelineStages = {stringIndexer, oneHotEncoder, vectorAssembler, scaler};
        //PipelineStage[] pipelineStages = {stringIndexer, oneHotEncoder, vectorAssembler};
        PipelineModel pipelineModel = new Pipeline().setStages(pipelineStages).fit(dataFrame);

        //最终特征
        DataFrame newfeature = pipelineModel.transform(dataFrame);
//        newfeature.show();
//        newfeature.printSchema();

        //save
        JavaRDD<LabeledPoint> labeledPointJavaRDD = newfeature.select("label", "scaledFeatures")
                .javaRDD()
                .map(row -> new LabeledPoint(row.getDouble(0), (DenseVector) row.get(1)));
        labeledPointJavaRDD.saveAsObjectFile(feature_save_file);

        jsc.close();
    }

    //生成节目特征
    private static JavaPairRDD<Long, double[]> generate_video_embedding_all(HashMap<String, Integer> indexmap) {
        return videopair
                .mapValues(video -> {
                    //创建一个空的onehot
                    double[] onehot = new double[indexmap.size()];

                    onehot[indexmap.get(video.category)] = 1.0;
                    for (String string : video.plots) {
                        onehot[indexmap.get(string)] = 1.0;
                    }
                    for (String string : video.regions) {
                        onehot[indexmap.get(string)] = 1.0;
                    }
                    return onehot;
                });
    }

    //生成用户特征
    private static JavaPairRDD<Long, double[]> generate_user_embedding_all(JavaPairRDD<Long, double[]> videofeatures) {
        return scores.mapToPair(score -> new Tuple2<>(score.videoid, score.userid))
                .join(videofeatures)
                .values()   // userid , videofeature
                .mapToPair(tuple -> tuple)   // userid : videofeature
                .reduceByKey((arr1, arr2) -> {
                    for (int i = 1; i < arr1.length; i++) {
                        if (arr1[i] == 1.0 || arr2[i] == 1.0) {
                            arr1[i] = 1.0;
                        }
                    }
                    return arr1;
                });
    }


}
