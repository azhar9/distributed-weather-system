package org.adt;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class WeatherDataProducer {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final Random random = new Random();

    private static final String[] LOCATIONS = {
            "windsor",
            "toronto",
            "vancouver",
            "montreal",
            "calgary",
            "ottawa"
    };

    private static final int NUM_SENSORS_PER_LOCATION = 5;
    public static final String KAFKA_TOPIC_NAME = "weather-data";
    public static Producer<String,String> getKafkaProducer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    public static void createKafkaTopic(String topicName) throws Exception {

        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");

        AdminClient adminClient = AdminClient.create(kafkaProps);

        //Check current list of topics
        Set<String> presentTopics = adminClient.listTopics().names().get();
        System.out.println("Current Topic List :" + presentTopics.toString());

        if (presentTopics.contains(topicName)) {
            System.out.println("Topic already exists : " + topicName);
            return;
        }

        System.out.println("Going to create Kafka topic " + topicName);
        NewTopic newTopic = new NewTopic(topicName, 1, (short) 1);

        List<NewTopic> newTopics = new ArrayList<NewTopic>();
        newTopics.add(newTopic);

        adminClient.createTopics(newTopics);
        adminClient.close();

    }

    public static void main(String[] args) throws Exception {
        createKafkaTopic(WeatherDataProducer.KAFKA_TOPIC_NAME);
        Producer<String, String> producer = getKafkaProducer();
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(() -> {
            try {
                for (String location : LOCATIONS) {
                    for (int i = 1; i <= NUM_SENSORS_PER_LOCATION; i++) {
                        String sensorId = location + "-sensor" + String.format("%02d", i);
                        long timestamp = System.currentTimeMillis() / 1000L;
                        double temperature = random.nextDouble() * 30 - 10; // generate temperature between -10 to 20 Celsius
                        double humidity = random.nextDouble() * 30 + 30; // generate humidity between 30-60%
                        double windSpeed = random.nextDouble() * 10 + 5; // generate wind speed between 5-15 m/s
                        String windDirection = generateWindDirection();

                        WeatherData data = new WeatherData(sensorId, location, timestamp, temperature, humidity, windSpeed, windDirection);

                        String json = mapper.writeValueAsString(data);
                        System.out.println(json);
                        producer.send(new ProducerRecord<>(KAFKA_TOPIC_NAME, json));
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private static String generateWindDirection() {
        String[] directions = {"N", "NE", "E", "SE", "S", "SW", "W", "NW"};
        return directions[random.nextInt(directions.length)];
    }

    private static class WeatherData {
        private final String sensorId;
        private final String location;
        private final long timestamp;
        private final double temperature;
        private final double humidity;
        private final double windSpeed;
        private final String windDirection;

        public WeatherData(String sensorId, String location, long timestamp, double temperature, double humidity, double windSpeed, String windDirection) {
            this.sensorId = sensorId;
            this.location = location;
            this.timestamp = timestamp;
            this.temperature = temperature;
            this.humidity = humidity;
            this.windSpeed = windSpeed;
            this.windDirection = windDirection;
        }

        public String getSensorId() {
            return sensorId;
        }

        public String getLocation() {
            return location;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getTemperature() {
            return temperature;
        }

        public double getHumidity() {
            return humidity;
        }

        public double getWindSpeed() {
            return windSpeed;
        }

        public String getWindDirection() {
            return windDirection;
        }
    }
}
