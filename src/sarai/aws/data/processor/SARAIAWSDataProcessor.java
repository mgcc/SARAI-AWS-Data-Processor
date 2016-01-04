/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor;

import com.mongodb.Block;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import java.util.ArrayList;
import java.util.List;
import org.bson.Document;
import sarai.aws.data.processor.collection.weatherlink.WeatherLinkDataCollector;

/**
 *
 * @author monina
 */
public class SARAIAWSDataProcessor {

    /**
     * @param args the command line arguments
     */
    
    ArrayList<Thread> weatherThreads;
    
    private void init() {
        weatherThreads = new ArrayList<>();
    }
    
    public SARAIAWSDataProcessor() {
        init();
        
        System.out.println("Starting AWS Data Processor...");
        
        MongoClient mongoClient = new MongoClient("localhost", 3001); //port should be in args
        MongoDatabase db = mongoClient.getDatabase("meteor");
        MongoCollection collection = db.getCollection("AutomaticWeatherStations");
//        MongoIterable<String> dbs = mongoClient.listDatabaseNames();        
//        System.out.println("Names: \n");
//        for (String name : dbs) {
//            System.out.println(name);
//        } 

        
        
        FindIterable<Document> weatherStations = collection.find();
        
        weatherStations.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                //System.out.println(document.getString("feed"));
                String feed = document.getString("feed");
                int interval = 1000;
                //Thread nt = new Thread(new WeatherLinkDataCollector(feed, interval));
                //weatherThreads.add(nt);
                Timer t = 
            }
        });
        
//        for (Thread t : weatherThreads) {
//            t.start();
//        }

    }
    
    public static void main(String[] args) {
        SARAIAWSDataProcessor sawsdp = new SARAIAWSDataProcessor();
    }
    
}
