package com.davidosantos.kafka.training.kafkatraining;

import java.text.DateFormat;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Logger;

import javax.annotation.PostConstruct;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

@SpringBootApplication
@EnableScheduling
public class KafkaTrainingApplication {

	Logger logger = Logger.getLogger(this.toString());

	Properties props = new Properties();
	KafkaProducer<String, Customers> producer;

	int count = 0;

	private Random random;

	public static void main(String[] args) {

		SpringApplication.run(KafkaTrainingApplication.class, args);
	}

	@PostConstruct
	void doingSetups() {
		logger.info("Doing setups..");
		this.props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka-1:19092,kafka-2:29092,kafka-3:39092");
		this.props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		this.props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

		// secure but less performant settings, duplicated and out of order messages does not happen
		
		//idempotent settings garantees that no out of order processing happens.
		//--> when true, the message will be sent to the broker with identifier PID:MessageSequenceNumber.
		this.props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE);
		//all broker must acknowledge the message --> must be all when idempotent settings is true.
		this.props.put(ProducerConfig.ACKS_CONFIG, "all");
		//a maximum number of 10 retry attempts are allowed.
		this.props.put(ProducerConfig.RETRIES_CONFIG, "10");
		//the broker will accept as much as 4 messsages without acknowledgements. --> must be between 1 - 5
		this.props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "4");


		this.props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://schema-registry:8081");
		//this.props.put(ProducerConfig.LINGER_MS_CONFIG, 5000);

		producer = new KafkaProducer<>(props);

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				this.logger.info("Flusing e Closing the producer...");
				this.producer.flush();
				this.producer.close();
			} catch (Throwable e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}));

	}

	// @Scheduled(fixedRate = 1000l)
	void updateKafka() {
		logger.info("Producing messages..");
//		this.producer.send(new ProducerRecord<String, String>("stream-topic-custom-serdes", "key", "value"));

	}

	@Scheduled(fixedRate = 1000l)
	void produceMessageWithLambdaCallbacks() {
		random = new Random();
		int age = random.ints(33, 36).findAny().getAsInt();
		int keyIndex = random.ints(0,4).findAny().getAsInt();
		String keys[] = {"random1", "random2", "random3", "random4"};
		
		// ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("stream-topic-custom-serdes", keys[keyIndex],
		// "{\"name\" : \"David\",\"age\" : "+age+",\"birthDate\" : \"1987-10-09T02:59:00.0000000+00:00\",\"timestamp\" : \""+ Instant.now()+"\"}");

		ProducerRecord<String, Customers> producerRecord = new ProducerRecord<String, Customers>("stream-topic-custom-serdes", keys[keyIndex],
		Customers.newBuilder()
		.setName("David")
		.setAge(age)
		.setBirthDate(Instant.now().toString())
		.setTimestamp(Instant.now().toString())
		.build());
		
		logger.info("ProducerRecord: " + producerRecord);
		
		producer.send(producerRecord, (recordMetadata, e) -> {

			if (e != null) {
				e.printStackTrace();
			} else {
				logger.info("Message Produced at offset: " + recordMetadata.offset() + " Partition "
						+ recordMetadata.partition());
			}
		});
	}

}
