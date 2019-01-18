package process.entity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.spark.mllib.linalg.Vector;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Feature implements Serializable {

    public Long userid;
    public Long videoid;

    public Double label;
    public String secondlevel;
    public Long watchnum;        //观看总次数
    public Double watchperc;       //观看总占比
    public double[] user_embedding;
    public double[] video_embedding;

    public Vector user_e;
    public Vector video_e;
}
