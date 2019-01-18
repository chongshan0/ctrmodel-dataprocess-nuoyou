package process.entity;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


@Data
@NoArgsConstructor
public class DoubanVideo implements Serializable {

    public List<String> names;

    public String category;
    public Double videotimelength;  //分钟

    public HashSet<String> directs;
    public HashSet<String> actors;
    public HashSet<String> plots;
    public HashSet<String> regions;

    public void setHashSet(String dir, String act, String plo, String reg) {
        directs = new HashSet<>();
        for (String str : dir.split("\\|")) {
            if (!str.trim().equals(""))
                directs.add(str.trim());
        }

        actors = new HashSet<>();
        for (String str : act.split("\\|")) {
            if (!str.trim().equals(""))
                actors.add(str.trim());
        }

        plots = new HashSet<>();
        for (String str : plo.split("\\|")) {
            if (!str.trim().equals("")) {
                Pattern pattern = Pattern.compile("[0-9]*");
                Matcher isNum = pattern.matcher(str);
                if (!isNum.matches()) {
                    plots.add(str.trim());
                }
            }
        }

        regions = new HashSet<>();
        for (String str : reg.split("\\|")) {
            if (!str.trim().equals(""))
                regions.add(str.trim());
        }
    }
}
