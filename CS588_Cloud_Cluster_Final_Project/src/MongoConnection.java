/**
 * @author sanjuktadas
 * create connection to MongoDB Atlas
 * read data from csv file and import 
 * data to collection in FreewayData database
 */

import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.MongoCredential;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import static com.mongodb.client.model.Filters.*;

/*
Questions:
1)   Count high speeds: Find the number of speeds > 100 in the data set.

2)   Volume: Find the total volume for the station Foster NB for Sept 21, 2011.

3)   Single-Day Station Travel Times: Find travel time for station Foster NB for 
5-minute intervals for Sept 22, 2011. Report travel time in seconds.

4)   Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM 
on September 22, 2011 for station Foster NB. Report travel time in seconds.

5)   Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM 
on September 22, 2011 for the I-205 NB freeway. Report travel time in minutes.

6)   Route Finding: Find a route from Johnson Creek to Columbia Blvd on I-205 
NB using the upstream and downstream fields.

*/

public class MongoConnection {
	public static final boolean debug = true;
	public MongoCollection readCollection = null;
	public MongoCollection loopCollection = null;
	
	/**
	 * this method establishes connection to MongoDB Atlas and returns a MongoCollection object
	 * @param void
	 * @return MongoCollection
	 */
	public void createConnection() {

		String atlas_uri = "mongodb+srv://Sanju:sanjuPW@mongocluster-80u2e.gcp.mongodb.net/test";

		String databaseName = "FreewayData";
		String loopCollectionName = "loopdata";
		String readCollectionName = "read_collection_1";

		MongoClientURI  uri;
		MongoClient mongoClient;
		MongoDatabase database;
		
		try {
			uri = new MongoClientURI(atlas_uri);
			mongoClient = new MongoClient(uri);
			database = mongoClient.getDatabase(databaseName);
			readCollection = database.getCollection(readCollectionName);
			loopCollection = database.getCollection(loopCollectionName);
			
			if(debug){
				System.out.println("readCollection = " + readCollection);
				System.out.println("loopCollection = " + loopCollection);
			}
		} catch (Exception e) {
			System.out.println("Failed to connect to MongoDB Atlas.");
		}
	}
//-----------------------------------------------------------------------------------------------------------------------
	/**
	 * @param args
	 * 2) - Volume: Find the total volume for the station Foster NB for Sept 21, 2011.
	 */
	public void q2() {
		if (loopCollection == null || readCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		int total_volume = 0;
		DateFormat format = new SimpleDateFormat("yyyy-dd-MM'T'HH:mm:ss'Z'", Locale.ENGLISH);
		
		FindIterable<Document> getDetectorIds = readCollection.find(eq("location_text", "Foster NB"));
		for(Document r_doc : getDetectorIds) {
			List<String> detectorIds =  (List<String>) r_doc.get("detector_id_array");
			for(String dId : detectorIds) {
				try {
					FindIterable<Document> queryResult = loopCollection.find(and(eq("detectorid", Integer.parseInt(dId)),
							gte("startime", format.parse("2011-21-09T00:00:00Z")),
							lt("startime", format.parse("2011-22-09T00:00:00Z"))));
					for(Document l_doc : queryResult) {
						total_volume += l_doc.getInteger("volume", 0);
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("station : Foster NB \nDate : Sept 21, 2019 \nTotal volume : " + total_volume);
	}
//-----------------------------------------------------------------------------------------------------------------------

	/*3)   Single-Day Station Travel Times: Find travel time for station Foster NB for 
	5-minute intervals for Sept 22, 2011. Report travel time in seconds.*/

	public void singleDayTravelTime() {
		double length = 1.65;
		if (loopCollection == null || readCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		int total_volume = 0;
		DateFormat format = new SimpleDateFormat("yyyy-dd-MM'T'HH:mm:ss'Z'", Locale.ENGLISH);
		
		FindIterable<Document> getDetectorIds = readCollection.find(eq("location_text", "Foster NB"));
		for(Document r_doc : getDetectorIds) {
			List<String> detectorIds =  (List<String>) r_doc.get("detector_id_array");
			for(String dId : detectorIds) {
				try {
					FindIterable<Document> queryResult = loopCollection.find(and(eq("detectorid", Integer.parseInt(dId)),
							gte("startime", format.parse("2011-22-09T00:00:00Z")),
							lt("startime", format.parse("2011-23-09T00:00:00Z"))));
					for(Document l_doc : queryResult) {
						
						
						
						
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("station : Foster NB \nDate : Sept 21, 2019 \nTotal volume : " + total_volume);
		
	}


	
//-----------------------------------------------------------------------------------------------------------------------

	/**
	 * @param args
	 * 4) - Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM on September 22, 2011 
	 * for station Foster NB. Report travel time in seconds.
	 */
	public void q4() {
		if (loopCollection == null || readCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		double travelTime7to9 = 0.0, travelTime4to6 = 0.0;
		int count7to9 = 0, count4to6 =0;
		double averageTravelTime7to9, averageTravelTime4to6;
		
		List<String> detectorIdList = null;
		DateFormat format = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss", Locale.ENGLISH);
		FindIterable<Document> docs = readCollection.find(eq("location_text", "Foster NB"));
		double stationLength = 0;
		for(Document r_doc : docs) {
			detectorIdList =  (List<String>) r_doc.get("detector_id_array");
			stationLength = Double.parseDouble((String) r_doc.get("length"));
		}
		
		List<Integer> detectorIdIntList = new ArrayList<Integer>();
		for(String strId : detectorIdList) detectorIdIntList.add(Integer.parseInt(strId));
		
		try {
			Bson filter7to9 = Filters.and(
		            Filters.in("detectorid", detectorIdIntList),
		            Filters.gte("startime", format.parse("2011-21-09 07:00:00")),
		            Filters.lt("startime", format.parse("2011-21-09 09:00:00")));
		    
		    Bson filter4to6 = Filters.and(
		            Filters.in("detectorid", detectorIdIntList),
		            Filters.gte("startime", format.parse("2011-21-09 16:00:00")),
		            Filters.lt("startime", format.parse("2011-21-09 18:00:00")));
		    
		    FindIterable<Document> loopdata7to9 = loopCollection.find(filter7to9);
		    for (Document doc : loopdata7to9) {
		    	if (doc.get("speed") != null) {
		    		travelTime7to9 += (stationLength / Double.parseDouble((String) doc.get("speed")));
		    		count7to9++;
		    	}
		    }
		    averageTravelTime7to9 = travelTime7to9 / count7to9;
		    System.out.println("averageTravelTime7to9 = " + averageTravelTime7to9);
		    
		    FindIterable<Document> loopdat4to6 = loopCollection.find(filter4to6);
		    for (Document doc : loopdat4to6) {
		    	if (doc.get("speed") != null) {
		    		travelTime4to6 += (stationLength / Double.parseDouble((String) doc.get("speed")));
		    		count4to6++;
		    	}
		    }
		    averageTravelTime4to6 = travelTime4to6 / count4to6;
		    System.out.println("averageTravelTime4to6 = " + averageTravelTime4to6);
		    
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

//-----------------------------------------------------------------------------------------------------------------------
/*
 5)   Peak Period Travel Times: Find the average travel time for 7-9AM and 4-6PM 
on September 22, 2011 for the I-205 NB freeway. Report travel time in minutes.
*/

	public void q5() {
		if (loopCollection == null || readCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		double travelTime7to9 = 0.0, travelTime4to6 = 0.0;
		int count7to9 = 0, count4to6 =0;
		double averageTravelTime7to9, averageTravelTime4to6;
		DateFormat format = new SimpleDateFormat("yyyy-dd-MM HH:mm:ss", Locale.ENGLISH);
		List<String> detectorIdList = null;
		List<Integer> detectorIdIntList = null;
		double stationLength = 0;
		double cumAverageTravelTime7to9 = 0, cumAverageTravelTime4to6 = 0;
		
		Bson filterHighways = Filters.and(
	            Filters.eq("highway_name", "I-205"),
	            Filters.eq("direction", "NORTH"));

		FindIterable<Document> highwayDocs = readCollection.find(filterHighways);
		for(Document h_doc : highwayDocs) {
			System.out.print("Station : " + h_doc.get("location_text"));
			stationLength = Double.parseDouble((String) h_doc.get("length"));
			detectorIdList =  (List<String>) h_doc.get("detector_id_array");
			for(String strId : detectorIdList) {
				detectorIdIntList.add(Integer.parseInt(strId));
			}
			try {
				Bson filter7to9 = Filters.and(
			            Filters.in("detectorid", detectorIdIntList),
			            Filters.gte("startime", format.parse("2011-21-09 07:00:00")),
			            Filters.lt("startime", format.parse("2011-21-09 09:00:00")));
			    
			    Bson filter4to6 = Filters.and(
			            Filters.in("detectorid", detectorIdIntList),
			            Filters.gte("startime", format.parse("2011-21-09 16:00:00")),
			            Filters.lt("startime", format.parse("2011-21-09 18:00:00")));
			    
			    FindIterable<Document> loopdata7to9 = loopCollection.find(filter7to9);
			    for (Document doc : loopdata7to9) {
			    	if (doc.get("speed") != null) {
			    		travelTime7to9 += (stationLength / Double.parseDouble((String) doc.get("speed")));
			    		count7to9++;
			    	}
			    }
			    averageTravelTime7to9 = travelTime7to9 / count7to9;
			    System.out.println("averageTravelTime7to9 = " + averageTravelTime7to9);
			    cumAverageTravelTime7to9 += averageTravelTime7to9;
			    
			    FindIterable<Document> loopdat4to6 = loopCollection.find(filter4to6);
			    for (Document doc : loopdat4to6) {
			    	if (doc.get("speed") != null) {
			    		travelTime4to6 += (stationLength / Double.parseDouble((String) doc.get("speed")));
			    		count4to6++;
			    	}
			    }
			    averageTravelTime4to6 = travelTime4to6 / count4to6;
			    System.out.println("averageTravelTime4to6 = " + averageTravelTime4to6);
			    cumAverageTravelTime4to6 += averageTravelTime4to6;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}		
	}

	
//-----------------------------------------------------------------------------------------------------------------------


	
	
//-----------------------------------------------------------------------------------------------------------------------

	public void printWriteCollection() {
		if (loopCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		FindIterable<Document> queryResult = loopCollection.find();
		for (Document doc : queryResult) {
			System.out.println(doc.toString());
        }
	}

//-----------------------------------------------------------------------------------------------------------------------
	
	public void printReadCollection() {
		if (readCollection == null) {
			System.out.println("collection is empty/null");
			return;
		}
		FindIterable<Document> queryResult = readCollection.find();
		for (Document doc : queryResult) {
			System.out.println(doc.toString());
        }
	}

//-----------------------------------------------------------------------------------------------------------------------

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		MongoConnection conn = new MongoConnection();
		conn.createConnection();
		//conn.printWriteCollection();		
		conn.q2();		// call function to run query-2
		conn.q4();
	}
}
