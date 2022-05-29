package vn.ifa.study.j2c.model;

import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsOutputDeliveryResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class ResultRecord extends KinesisFirehoseEvent.Record {
    private KinesisAnalyticsOutputDeliveryResponse.Result result;
}
