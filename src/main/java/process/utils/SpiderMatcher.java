package process.utils;

import process.entity.DoubanVideo;
import process.entity.Video;

public class SpiderMatcher {

    //判断是否匹配
    public static boolean isMatch(DoubanVideo doubanVideo, Video video) {
        //节目表的节目名
        String targetname = StringUtil.formatVideoName(video.videoname);

        for (String name : doubanVideo.names) {
            //处理"魔神英雄传 魔神英雄伝ワタル"
            String[] splitedname = name.split(" ");
            for (String divname : splitedname) {
                String currname = StringUtil.formatVideoName(divname);
                if (currname.equals(targetname)) {
                    return true;
                }
            }
        }
        return false;
    }
}
