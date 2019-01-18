package process.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class SimpleRecord implements Serializable {

    public String rowkey;
    public String videoname;
    public Long watchtime;    //毫秒
    public Integer ht;   //eg. 2018101600

    public SimpleRecord(String line) {
        String[] sarr = line.split(",");
        rowkey = sarr[1];
        videoname = sarr[2];
        watchtime = Long.parseLong(sarr[3]);
        ht = Integer.parseInt(sarr[4]);
    }
}
