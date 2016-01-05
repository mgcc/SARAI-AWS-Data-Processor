/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor;

import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import java.util.ArrayList;
import java.util.Date;
import java.util.Timer;
import org.bson.Document;
import sarai.aws.data.processor.collection.weatherlink.WeatherLinkDataCollector;
import sarai.aws.data.processor.storage.WeatherDataStorer;

/**
 *
 * @author monina
 */
public class SARAIAWSDataProcessor {

    /**
     * @param args the command line arguments
     */
    
    ArrayList<Timer> weatherTimers;
    
    
    
    private void init() {
        weatherTimers = new ArrayList<>();
    }
    
    public SARAIAWSDataProcessor() {
        init();
        
        System.out.println("Starting AWS Data Processor...");
        
        MongoClient mongoClient = new MongoClient("localhost", 3001); //port should be in args
        MongoDatabase db = mongoClient.getDatabase("meteor");
        MongoCollection awsCollection = db.getCollection("AutomaticWeatherStations");
        
        MongoCollection weatherCollection = db.getCollection("WeatherData");
        
        //Create single connection to weather collection for all to share.
        WeatherDataStorer dataStorer = new WeatherDataStorer(weatherCollection);
       
        FindIterable<Document> weatherStations = awsCollection.find();
        
        weatherStations.forEach(new Block<Document>() {
            @Override
            public void apply(final Document document) {
                
                //Get relevant data from document
                String feed = document.getString("feed");
                int interval = ((Number) document.get("interval")).intValue();
                
                //Create new Timer class to add to the list
                //@TODO: Might need separate timers for daily, monthly, and yearly values.
                Timer t = new Timer();
                t.scheduleAtFixedRate(new WeatherLinkDataCollector(feed, dataStorer), new Date(), interval);
                        
                //@TODO: At timer creation, fetch data right away, then settle into a 'X minutes after the hour' routine.
                weatherTimers.add(t);
                
            }
        });

    }
    
    public static void main(String[] args) {
        SARAIAWSDataProcessor sawsdp = new SARAIAWSDataProcessor();
    }
    
}
