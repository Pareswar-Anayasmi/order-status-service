package com.anayasmi.order.consumer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.bson.Document;
import java.util.Map;

@Service
@Slf4j
public class OrderStatusConsumer {

    @KafkaListener(topics = "order-status-topic",  groupId = "my-group-id")
    public void listen(String message) {

        log.info("Received Message: " + message);
    }

    private final ObjectMapper objectMapper = new ObjectMapper();
    @KafkaListener(topics = "order-pdf-topic", groupId = "my-group-id")
    public void handleFileUpload(ConsumerRecord<String, String> record) {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017")) {

            String jsonValue = record.value();
            if (jsonValue == null || jsonValue.trim().isEmpty()) {
                log.error("❌ Received empty or null payload.");
                return;
            }
            Map<String, Object> fileData = objectMapper.readValue(jsonValue, Map.class);

            String fileName = (String) fileData.get("fileName");
            String extension = (String) fileData.get("extension");
            Number size = (Number) fileData.get("size");
            String base64 = (String) fileData.get("content");
            log.info("✅ Consumer read PDF from Kafka: " + fileName);

            if (fileName == null || base64 == null) {
                log.error("❌ Missing fileName or content in payload.");
                return;
            }

            MongoDatabase database = mongoClient.getDatabase("kafka_poc");
            MongoCollection<Document> collection = database.getCollection("order_pdf_file");

            Document document = new Document()
                    .append("fileName", fileName)
                    .append("extension", extension)
                    .append("size", size != null ? size.longValue() : 0)
                    .append("base64", base64);

            collection.insertOne(document);
            log.info("✅ PDF Stored to MongoDB: " + fileName);

        } catch (Exception e) {
            log.error("❌ Failed to process message: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
