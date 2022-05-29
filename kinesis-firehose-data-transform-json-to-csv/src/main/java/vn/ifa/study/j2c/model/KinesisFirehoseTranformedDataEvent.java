package vn.ifa.study.j2c.model;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

@Setter
@Getter
public class KinesisFirehoseTranformedDataEvent  implements Serializable {

    private String invocationId;
    private String deliveryStreamArn;
    private String region;
    private List<ResultRecord> records;

    public List<ResultRecord> getRecords() {
        return records;
    }
}
