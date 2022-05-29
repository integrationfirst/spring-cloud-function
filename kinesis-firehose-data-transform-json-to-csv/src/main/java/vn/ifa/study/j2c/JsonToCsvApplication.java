package vn.ifa.study.j2c;

import com.amazonaws.services.lambda.runtime.events.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import vn.ifa.study.j2c.model.KinesisFirehoseTranformedDataEvent;
import vn.ifa.study.j2c.model.ResultRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@SpringBootApplication
public class JsonToCsvApplication {

    public static final String NEW_LINE_DELIMITER = "\n";

    public static void main(String[] args) {
        SpringApplication.run(JsonToCsvApplication.class, args);
    }

    private ObjectMapper mapper = new ObjectMapper();

    @Bean
    public Function<KinesisFirehoseEvent, KinesisFirehoseTranformedDataEvent> jsonToCSV() {
        return e -> {
            log.info("Start transform Kinesis Firehose event with invocation id {}", e.getInvocationId());
            KinesisFirehoseTranformedDataEvent result = from(e);
            result.getRecords().forEach(mapEachRecordToJSON);
            log.info("Complete transform event");
            return result;
        };
    }

    private final KinesisFirehoseTranformedDataEvent from(KinesisFirehoseEvent fe) {
        final KinesisFirehoseTranformedDataEvent e = new KinesisFirehoseTranformedDataEvent();

        e.setInvocationId(fe.getInvocationId());
        e.setRegion(fe.getRegion());
        e.setDeliveryStreamArn(fe.getDeliveryStreamArn());
        final List<ResultRecord> records = fe.getRecords().stream().map(toResultRecord).collect(Collectors.toList());
        e.setRecords(records);

        return e;
    }

    private Function<KinesisFirehoseEvent.Record, ResultRecord> toResultRecord = e -> {
        final ResultRecord rr = new ResultRecord();

        rr.setRecordId(e.getRecordId());
        rr.setApproximateArrivalEpoch(e.getApproximateArrivalEpoch());
        rr.setApproximateArrivalTimestamp(e.getApproximateArrivalTimestamp());
        rr.setKinesisRecordMetadata(e.getKinesisRecordMetadata());
        rr.setResult(KinesisAnalyticsOutputDeliveryResponse.Result.DeliveryFailed);
        rr.setData(e.getData());

        return rr;
    };

    private Consumer<ResultRecord> mapEachRecordToJSON = (e) -> {

        try {
            JsonNode jsonNode = mapper.readTree(e.getData().array());
            String csvContent = jsonToCSVRecord(jsonNode);
            byte[] csvRecordContent = csvContent.getBytes();
            e.setData(ByteBuffer.wrap(csvRecordContent));
            e.setResult(KinesisAnalyticsOutputDeliveryResponse.Result.Ok);
            log.info("Transform record with id {}", e.getRecordId());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    };

    private String jsonToCSVRecord(JsonNode data) {
        if (data == null || data.isEmpty()) {
            return null;
        }

        String traceId = data.findValue("traceId").textValue();
        String eventId = data.findValue("eventId").textValue();
        String status = data.findValue("status").textValue();
        String eventTime = data.findValue("eventTime").textValue();

        return String.join(";", traceId, eventId, status, eventTime) + NEW_LINE_DELIMITER;
    }
}
