package process.utils;


import org.apache.spark.api.java.JavaRDD;
import process.entity.DoubanVideo;

import java.util.ArrayList;

public class Cleaner {

    //清洗豆瓣数据
    public static JavaRDD<DoubanVideo> doubanCleaner(JavaRDD<String> doubanLines) {
        return doubanLines.map(s -> s.split(","))
                .filter(x -> x.length == 8)    //拆分并验证数据的列数
                .map(sarr -> {
                    //所有字段trim，转换分隔符
                    for (int i = 0; i < sarr.length; i++)
                        sarr[i] = sarr[i].trim().replace('/', '|');
                    return sarr;
                })
                .filter(sarr -> {
                    //验证数值类型
                    try {
                        Integer.parseInt(sarr[3]);
                        return true;
                    } catch (Exception e) {
                        return false;
                    }
                })
                .map(sarr -> {
                    DoubanVideo doubanVideo = new DoubanVideo();
                    doubanVideo.category = sarr[2];
                    doubanVideo.videotimelength = Double.parseDouble(sarr[3]);
                    doubanVideo.setHashSet(sarr[5], sarr[6], sarr[4], sarr[7]);
                    doubanVideo.names = new ArrayList<>();
                    doubanVideo.names.add(sarr[0]);
                    for (String str : sarr[1].split("\\|")) {
                        if (!str.trim().equals(""))
                            doubanVideo.names.add(str.trim());
                    }
                    return doubanVideo;
                });
    }

}
