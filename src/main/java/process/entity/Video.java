package process.entity;

import lombok.Data;
import lombok.NoArgsConstructor;
import process.utils.StringUtil;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;


@Data
@NoArgsConstructor
public class Video implements Serializable {

    public Long videoid;
    public String videoname;
    public String category; //secondlevel
    public HashSet<String> directs;
    public HashSet<String> actors;
    public HashSet<String> plots;
    public HashSet<String> regions;
    public Double videotimelength;  //分钟

    public Video(String line) {
        String[] sarr = line.split(",");
        videoid = Long.parseLong(sarr[0]);
        videoname = sarr[1];
        category = sarr[2];
        directs = StringUtil.splitToSet(sarr[3]);
        actors = StringUtil.splitToSet(sarr[4]);
        plots = StringUtil.splitToSet(sarr[5]);
        regions = StringUtil.splitToSet(sarr[6]);
        videotimelength = Double.parseDouble(sarr[7]);
    }

    public void setHashSet(List<String> dir, List<String> act, List<String> plo, List<String> reg) {
        directs = new HashSet<>();
        directs.addAll(dir);

        actors = new HashSet<>();
        actors.addAll(act);

        plots = new HashSet<>();
        plots.addAll(plo);

        regions = new HashSet<>();
        regions.addAll(reg);
    }

    //写文件字符串
    public String toSimpleString() {
        String result = videoid + "," + videoname + "," + category;
        result += "," + hashsetToString(directs);
        result += "," + hashsetToString(actors);
        result += "," + hashsetToString(plots);
        result += "," + hashsetToString(regions);
        result += "," + videotimelength;
        return result;
    }

    //hashset转为以|分隔的string
    private String hashsetToString(HashSet<String> hashSet) {
        String result = "";
        for (String str : hashSet) {
            result += "|" + str;
        }
        if (result.startsWith("|")) {
            result = result.substring(1);
        }
        return result;
    }
}
