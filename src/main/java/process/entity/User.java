package process.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;


@Data
@NoArgsConstructor
@AllArgsConstructor
public class User implements Serializable {

    public Long userid;
    public String rowkey;

    public User(String line) {
        String[] sarr = line.split(",");
        userid = Long.parseLong(sarr[0]);
        rowkey = sarr[1];
    }

    public String toSimpleString() {
        return userid + "," + rowkey;
    }
}
