package vn.ifa.study.j2c;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.amazonaws.services.lambda.runtime.events.KinesisAnalyticsOutputDeliveryResponse;
import com.amazonaws.services.lambda.runtime.events.KinesisFirehoseEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.extern.slf4j.Slf4j;
import vn.ifa.study.j2c.model.KinesisFirehoseTranformedDataEvent;
import vn.ifa.study.j2c.model.ResultRecord;

@Slf4j
@SpringBootApplication
public class JsonToCsvApplication {

	public static final String NEW_LINE_DELIMITER = "\n";

	public static void main(final String[] args) {
		SpringApplication.run(JsonToCsvApplication.class, args);
	}

	private final ObjectMapper mapper = new ObjectMapper();

	@Bean
	public Function<KinesisFirehoseEvent, KinesisFirehoseTranformedDataEvent> jsonToCSV() {
		return e -> {
			log.info("Start transform Kinesis Firehose event with invocation id {}", e.getInvocationId());
			final KinesisFirehoseTranformedDataEvent result = this.from(e);
			result	.getRecords()
					.forEach(this.mapEachRecordToJSON);
			log.info("Complete transform event");
			return result;
		};
	}

	private final KinesisFirehoseTranformedDataEvent from(final KinesisFirehoseEvent fe) {
		final KinesisFirehoseTranformedDataEvent e = new KinesisFirehoseTranformedDataEvent();

		e.setInvocationId(fe.getInvocationId());
		e.setRegion(fe.getRegion());
		e.setDeliveryStreamArn(fe.getDeliveryStreamArn());
		final List<ResultRecord> records = fe	.getRecords()
												.stream()
												.map(this.toResultRecord)
												.collect(Collectors.toList());
		e.setRecords(records);

		return e;
	}

	private final Function<KinesisFirehoseEvent.Record, ResultRecord> toResultRecord = e -> {
		final ResultRecord rr = new ResultRecord();

		rr.setRecordId(e.getRecordId());
		rr.setApproximateArrivalEpoch(e.getApproximateArrivalEpoch());
		rr.setApproximateArrivalTimestamp(e.getApproximateArrivalTimestamp());
		rr.setKinesisRecordMetadata(e.getKinesisRecordMetadata());
		rr.setResult(KinesisAnalyticsOutputDeliveryResponse.Result.DeliveryFailed);
		rr.setData(e.getData());

		return rr;
	};

	private final Consumer<ResultRecord> mapEachRecordToJSON = e -> {

		try {
			final JsonNode jsonNode = this.mapper.readTree(e.getData()
															.array());
			final String csvContent = this.jsonToCSVRecord(jsonNode);
			final byte[] csvRecordContent = csvContent.getBytes();
			e.setData(ByteBuffer.wrap(csvRecordContent));
			e.setResult(KinesisAnalyticsOutputDeliveryResponse.Result.Ok);
			log.info("Transform record with id {}", e.getRecordId());
		} catch (final IOException ex) {
			throw new RuntimeException(ex);
		}
	};

	private String jsonToCSVRecord(final JsonNode data) {
		if ((data == null) || data.isEmpty()) {
			return null;
		}

		final String traceId = data	.findValue("traceId")
									.textValue();
		final String eventId = data	.findValue("eventId")
									.textValue();
		final String status = data	.findValue("status")
									.textValue();
		final String eventTime = data	.findValue("eventTime")
										.textValue();

		return String.join(";", traceId, eventId, status, eventTime) + NEW_LINE_DELIMITER;
	}
}
