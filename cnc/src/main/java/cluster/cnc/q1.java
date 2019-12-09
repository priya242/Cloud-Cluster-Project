package cluster.runner;

import java.time.Duration;
import java.time.Instant;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class q1 {
	public static void main(String[] args) {
		MongoClient mongo = new MongoClient("localhost", 27017);
		MongoDatabase database = mongo.getDatabase("mongodb");
		MongoCollection<Document> loopcollection = database.getCollection("loopdata1");
		long result = loopcollection.countDocuments(Filters.gt("speed", 100));
		mongo.close();
	}
}
