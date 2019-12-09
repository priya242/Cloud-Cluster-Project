package cluster.runner;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bson.Document;

public class Start {
	public static void main(String[] args) {
		/**** Read from CSV ****/
		String highwayFile = "C:/Users/vikramguhilot/Downloads/highways.csv";
		String stationsFile = "C:/Users/vikramguhilot/Downloads/freeway_stations.csv";
		String detectorsFile = "C:/Users/vikramguhilot/Downloads/freeway_detectors.csv";
		String loopDataFile = "C:/Users/vikramguhilot/Downloads/freeway_loopdata.csv";
		BufferedReader A_BRH = null;
		BufferedReader A_BRS = null;
		BufferedReader A_BRD = null;
		BufferedReader A_BR = null;
		String A_HL = "";
		String A_SL = "";
		String A_DL = "";
		String loop = "";
		String cvsSplitBy = ",";

		
		MongoClient mongo = new MongoClient("localhost", 27017);
		
		MongoCredential credential;
		credential = MongoCredential.createCredential("sampleUser", "myDb", "password".toCharArray());
		System.out.println("Connected to the database successfully");
	
		MongoDatabase database = mongo.getDatabase("mongodb");

	
		MongoCollection<Document> collection = database.getCollection("highways");
		try {
			A_BRH = new BufferedReader(new FileReader(highwayFile));
			Document highway_document;
			Document station_document;
			Document detector_document;
			Document loop_document;

			while ((A_HL = A_BRH.readLine()) != null) {
				List<Document> stationdocuments = new ArrayList<Document>();
				String[] highway = A_HL.split(cvsSplitBy);
				highway_document = new Document("highwayid", highway[0]).append("shortdirection", highway[1])
						.append("direction", highway[2]).append("highwayname", highway[3]);
				String highwayid = highway[0];

				A_BRS = new BufferedReader(new FileReader(stationsFile));
				while ((A_SL = A_BRS.readLine()) != null) {
					String[] station = A_SL.split(cvsSplitBy);
					String stationid = station[0];
					if (station[1] != null && !station[1].isEmpty() && station[1].equalsIgnoreCase(highwayid)) {
						station_document = new Document("stationid", station[0]).append("highwayid", station[1])
								.append("milepost", station[2]).append("locationtext", station[3])
								.append("upstream", station[4]).append("downstream", station[5])
								.append("stationclass", station[6]).append("numberlanes", station[7])
								.append("latlon", station[8] + "," + station[9]).append("length", station[10]);

						A_BRD = new BufferedReader(new FileReader(detectorsFile));
						List<Document> detectordocuments = new ArrayList<Document>();
						while ((A_DL = A_BRD.readLine()) != null) {
							String[] detector = A_DL.split(cvsSplitBy);
							if (detector[6] != null && !detector[6].isEmpty()
									&& detector[6].equalsIgnoreCase(stationid)) {
								detector_document = new Document("detectorid", detector[0])
										.append("highwayid", detector[1]).append("milepost", detector[2])
										.append("locationtext", detector[3]).append("detectorclass", detector[4])
										.append("lanenumber", detector[5]).append("stationid", detector[6]);
								detectordocuments.add(detector_document);
							}
						}
						station_document.append("detectors", detectordocuments);
						stationdocuments.add(station_document);
					}

				}
				highway_document.append("stations", stationdocuments);
				collection.insertOne(highway_document);
			}
			MongoCollection<Document> loopCollection = database.getCollection("loopdata1");
			A_BR = new BufferedReader(new FileReader(loopDataFile));

			Date startDate = new Date();
			SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			while ((loop = A_BR.readLine()) != null) {
				String[] loop_record = loop.split(cvsSplitBy);
				try {
					startDate = sd.parse(loop_record[1].substring(0,18));

				} catch (ParseException e) {
					
					e.printStackTrace();
				}
				loop_document = new Document("detectorid", loop_record[0] != null ? Integer.parseInt(loop_record[0]) : null)
						.append("starttime", startDate.getTime())
						.append("volume", loop_record[2].equals("") ? null : Integer.parseInt(loop_record[2]))
						.append("speed", loop_record[3].equals("") ? null : Integer.parseInt(loop_record[3]))
						.append("occupany", loop_record[4].equals("") ? null : Integer.parseInt(loop_record[4]))
						.append("status", loop_record[5].equals("") ? "" : Integer.parseInt(loop_record[5]))
						.append("dqflags", loop_record[6].equals("") ? "" : Integer.parseInt(loop_record[6]));
				loopCollection.insertOne(loop_document);
			}	
			System.out.println("Execution finished!!");

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (A_BRH != null) {
				try {
					A_BRH.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (A_BRS != null) {
				try {
					A_BRS.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			if (A_BRD != null) {
				try {
					A_BRD.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
}
