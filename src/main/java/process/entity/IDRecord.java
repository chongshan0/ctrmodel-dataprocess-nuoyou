package process.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class IDRecord implements Serializable {

    public Long userid;
    public Long videoid;
    public Long watchtime;    //毫秒
    public Integer ht;   //eg. 2018101600

    public IDRecord(String line) {
        String[] sarr = line.split(",");
        userid = Long.parseLong(sarr[0]);
        videoid = Long.parseLong(sarr[1]);
        watchtime = Long.parseLong(sarr[2]);
        ht = Integer.parseInt(sarr[3]);
    }


    public String toWriteLine() {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(userid).append(",");
        stringBuffer.append(videoid).append(",");
        stringBuffer.append(watchtime).append(",");
        stringBuffer.append(ht);
        return stringBuffer.toString();
    }


}
