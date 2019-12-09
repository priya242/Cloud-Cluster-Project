/**
 * @author sanjuktadas
 * create connection to MongoDB Atlas
 * read data from csv file and import 
 * data to collection in FreewayData database
 */

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
//import com.mongodb.ServerAddress;
//import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import java.util.Arrays;
import java.io.*;

public class MongoConnection {
	public static final boolean debug = true;	

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
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
}
