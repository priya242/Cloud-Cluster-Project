/**
 * @author sanjuktadas
 * create connection to MongoDB Atlas
 * read data from csv file and import 
 * data to collection in FreewayData database
 */
import org.bson.Document;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
//import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import java.util.ArrayList;
import java.util.Arrays;
import java.io.*;
import static com.mongodb.client.model.Filters.*;

public class MongoConnection {
	public static final boolean debug = true;	
	
	/**
	 * this method establishes connection to MongoDB Atlas and returns a MongoCollection object
	 * @param void
	 * @return MongoCollection
	 */
	public MongoCollection createConnection() {
		String atlas_uri = "mongodb+srv://mongocluster-80u2e.gcp.mongodb.net/test";
		String username = "Sanju";
		String password = "sanjuPW";

		String databaseName = "FreewayData";
		String collectionName = "write_collection_7";

		String folderPath = "/Users/sanjuktadas/Desktop/OneDrive/_PSU/courses/2019/FALL_2019/CS588-Cloud_and_Cluster_Data_Management/Final_Projects/cs588-final-project/cs588-project-freeway_data/";
		String highways = "highways.csv";
		String freeway_stations = "freeway_stations.csv";
		String freeway_detectors = "freeway_detectors.csv";
		String freeway_loopdata = "freeway_loopdata.csv";

		MongoClientURI  uri;
		MongoClient mongoClient;
		MongoDatabase database;
		MongoCollection dbCollection;

		try {
			uri = new MongoClientURI(atlas_uri);
			mongoClient = new MongoClient(uri);
			database = mongoClient.getDatabase(databaseName);
			dbCollection = database.getCollection(collectionName);
			if(debug){
				System.out.println("dbCollection = " + dbCollection);
			}
		} catch (Exception e) {
			System.out.println("Failed to connect to MongoDB Atlas.");
		}
	}
	
	/**
	 * @param args
	 * 2) - Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
	 */
	public void q2(MongoCollection coll) {
		if (coll == null) {
			System.out.println("collection is empty/null");
			return;
		}
		int total_volume = 0;
		ArrayList<Document> queryResult = coll.find(and(eq("locationtext", "Foster NB"), eq("StartDate", "2011-21-09")));
		for(Document doc : queryResult) {
			total_volume += doc.get("volume");
		}
		System.out.println("station : Foster NB \n Date : Sept 21, 2019 \n Total volume : " + total_volume);
	}
	
	/**
	 * @param args
	 * 4) - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 
	 * for station Foster NB. Report travel time in seconds.
	 */
	public void q4(MongoCollection coll) {
		if (coll == null) {
			System.out.println("collection is empty/null");
			return;
		}

		ArrayList<Document> query7to9 = coll.find(and(eq("locationtext", "Foster NB"), eq("StartDate", "2011-22-09"), gte("time", "7am"), lte("time", "9am")));
		ArrayList<Document> query4to6 = coll.find(and(eq("locationtext", "Foster NB"), eq("StartDate", "2011-22-09"), gte("time", "4pm"), lte("time", "6pm")));
		double length = 1.65;
		double totalTravelTime7to9 = 0.0, totalTravelTime4to6 = 0.0;
		double averageTravelTime7to9, averageTravelTime4to6; 
		double temp = 0.0, speed = 0.0;
		for (Document doc : query7to9) {
			speed = doc.get("speed");
			if(speed > 0) {
				temp = length * speed;
				totalTravelTime7to9 += temp;
			}
		}
		temp = 0.0;
		speed = 0.0;
		for (Document doc : query4to6) {
			speed = doc.get("speed");
			if(speed > 0) {
				temp = length * speed;
				totalTravelTime4to6 += temp;
			}
		}
		averageTravelTime7to9 = totalTravelTime7to9 / 7200;
		averageTravelTime4to6 = totalTravelTime4to6 / 7200;
		System.out.println("Average Travel Time : ");
		System.out.println("7 to 9 AM : " + averageTravelTime7to9 + "seconds");
		System.out.println("4 to 6 PM : " + averageTravelTime4to6 + "seconds");
	}
	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoConnection conn = new MongoConnection();
		MongoCollection coll = conn.createConnection();
		
		conn.q2(coll);		// call function to run query-2
	}
}
