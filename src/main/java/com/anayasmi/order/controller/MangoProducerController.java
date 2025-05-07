package com.anayasmi.order.controller;

import com.anayasmi.order.consumer.OrderStatusProducer;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CrossOrigin("*")
@RestController
@Slf4j
@RequiredArgsConstructor
public class MangoProducerController {
    String mongoUrl = "mongodb://localhost:27017";

    @Autowired
     private OrderStatusProducer orderStatusProducer;

    @GetMapping("/orders")
    public List<Document> fetchAllOrders() {
        List<Document> ordersList = new ArrayList<>();

        try (MongoClient mongoClient = MongoClients.create(mongoUrl)) {
            MongoDatabase database = mongoClient.getDatabase("kafka_poc");
            MongoCollection<Document> collection = database.getCollection("Orders");

            for (Document doc : collection.find()) {
                if (doc == null) continue;

                String objectIdString = null;
                Object objectIdRaw = doc.get("_id");

                if (objectIdRaw instanceof ObjectId) {
                    objectIdString = ((ObjectId) objectIdRaw).toHexString();
                } else if (objectIdRaw instanceof Document) {
                    // _id stored as Document with $oid key
                    Document idDoc = (Document) objectIdRaw;
                    objectIdString = idDoc.getString("$oid");
                }

                Document payload = doc.get("payload", Document.class);
                if (payload != null && objectIdString != null) {
                    Document after = payload.get("after", Document.class);
                    if (after != null) {
                        after.put("id", objectIdString); // Attach string id
                        ordersList.add(after);
                    }
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }

        return ordersList;
    }

    @PutMapping("/orders/{id}")
    public Map<String, Object> updateOrderFieldsById(
            @PathVariable String id,
            @RequestBody Map<String, Object> requestData
    ) {
        Map<String, Object> response = new HashMap<>();

        try (MongoClient mongoClient = MongoClients.create(mongoUrl)) {
            MongoDatabase database = mongoClient.getDatabase("kafka_poc");
            MongoCollection<Document> collection = database.getCollection("Orders");
            log.info("Update Request called.");
            // Convert String ID to ObjectId
            ObjectId objectId = new ObjectId(id);

            Document filter = new Document("_id", objectId);

            // Prepare update fields inside payload.after
            Document updateFields = new Document();

            if (requestData.containsKey("order_status")) {
                updateFields.append("payload.after.order_status", requestData.get("order_status"));
            }

            Document update = new Document("$set", updateFields);
            var result = collection.updateOne(filter, update);

            response.put("matchedCount", result.getMatchedCount());
            response.put("modifiedCount", result.getModifiedCount());
            response.put("message", "Order updated successfully");


            Object orderIdObj = requestData.get("order_id");
            Object customOrderIdObj = requestData.get("order_custom_id");
            Object statusObj = requestData.get("order_status");

            String orderId = orderIdObj != null ? orderIdObj.toString() : null;
            String customOrderId = customOrderIdObj != null ? customOrderIdObj.toString() : null;
            String status = statusObj != null ? statusObj.toString() : null;
            log.info("✅Order : "+customOrderId + status+ " successfully in MongoDB."+customOrderId);
            log.info("✅Update request sent to Kafka."+customOrderId);

            String json = String.format(
                    "{\"orderId\":\"%s\", \"CustomOrderId\":\"%s\", \"status\":\"%s\"}",
                    orderId, customOrderId, status
            );
            System.out.println(json);
            orderStatusProducer.sendMessage(json);


        } catch (Exception e) {
            e.printStackTrace();
            response.put("error", e.getMessage());
        }

        return response;
    }

}
