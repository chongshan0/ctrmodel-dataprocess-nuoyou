package process.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class Score implements Serializable {

    public Long userid;
    public Long videoid;
    public Double watchtimesum; //观看总时长，分钟
    public Double percentage;    //观看百分比
    public Double rui;  // rui = log(1+percentage)
    public Long watchnumsum;   //观看总次数

    public Score(String line) {
        String[] sarr = line.split(",");
        userid = Long.parseLong(sarr[0]);
        videoid = Long.parseLong(sarr[1]);
        watchtimesum = Double.parseDouble(sarr[2]);
        percentage = Double.parseDouble(sarr[3]);
        rui = Double.parseDouble(sarr[4]);
        watchnumsum = Long.parseLong(sarr[5]);
    }


    public String toWriteLine() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(userid).append(",");
        stringBuffer.append(videoid).append(",");
        stringBuffer.append(watchtimesum).append(",");
        stringBuffer.append(percentage).append(",");
        stringBuffer.append(rui).append(",");
        stringBuffer.append(watchnumsum);
        return stringBuffer.toString();
    }


}
