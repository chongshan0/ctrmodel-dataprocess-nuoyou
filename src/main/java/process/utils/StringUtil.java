package process.utils;

import java.util.HashSet;

public class StringUtil {

    //格式化节目名
    public static String formatVideoName(String videoName) {
        //转换小写
        videoName = videoName.toLowerCase().trim();

        //中文括号转英文
        videoName = videoName.replaceAll("（", "(");
        videoName = videoName.replaceAll("）", ")");

        //删除停用词字串（有前后顺序）
        String[] stopWords = {
                "版", "(美) ", "(台) ", "(港) ", "(港 / 台)", "(台 / 港)", "(日本) ",
                "(精选) ", "(1080p 4m 新)", "(中文)", "(美版)", "4k", "1080p", "杜比",
                "蓝光", "4m", "新4m", "4.5m", "高清", "限时免费", "限时特供", "限免",
                "豆友译名", "1元看大片", "原声", "英语", "未删减", "国语", "英文", "中文",
                "日文", "粤语", "tv", "国际", "抢先", "完整", "原声", "合集一", "合集二",
                "合集三", "合集四", "合集五", "合集", "精切", "方言", "片段1", "片段2",
                "片段3", "精选", "花絮", "片段", "泰语", "选段", "普通话", "会员", "日语"};
        for (String stopword : stopWords) {
            if (videoName.contains(stopword)) {
                videoName = videoName.replaceAll(stopword, "");
            }
        }

        //去除分隔符
        videoName = videoName.replaceAll("[\\pZ]", "");
        //去除标点字符
        videoName = videoName.replaceAll("[\\pP]", "");
        //去除符号如数学、货币符号
        videoName = videoName.replaceAll("[\\pS]", "");

        videoName = videoName
                .replaceAll(" ", "")
                .replaceAll("\t", "")
                .replaceAll("\n", "")
                .replaceAll("\r", "");

        return videoName.trim();
    }

    //拆分以“|”分隔的字符串为Set
    public static HashSet<String> splitToSet(String input) {
        //拆分为数组
        String[] sarr = input.split("\\|");
        //转换为set
        HashSet<String> results = new HashSet<>();
        for (String str : sarr) {
            String substr = str.trim();
            if (!substr.equals("")) {
                results.add(substr);
            }
        }
        return results;
    }


}
