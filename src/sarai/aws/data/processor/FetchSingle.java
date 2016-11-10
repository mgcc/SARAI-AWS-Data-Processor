/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor;

import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import com.mongodb.client.model.UpdateOptions;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONObject;
import sarai.aws.data.processor.constructor.WeatherRecordConstructor;

/**
 *
 * @author monina
 */
public class FetchSingle {
    
    MongoClient mongoClient;
    MongoDatabase db;
    
    MongoCollection weatherData;
    MongoCollection settings;
    String apiKey;
    
    String host;
    int port;
    String dbName;
    String stationID;
    String dateString;
    
    
    
    
    private void init() {
        mongoClient = new MongoClient(host, port);
        db = mongoClient.getDatabase(dbName);

        weatherData = db.getCollection("weather-data");
        settings = db.getCollection("dss-settings");
        
        Document wundergroundAPIKey = (Document) settings.find(eq("name", "wunderground-api-key")).first();
        apiKey = wundergroundAPIKey.getString("value");
    }
    
    public FetchSingle(String host, int port, String dbName, String stationID, String dateString) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.stationID = stationID;
        this.dateString = dateString;
        
        init();
    }
    
    public boolean fetch() throws MalformedURLException, IOException {
        String apiCallString = "http://api.wunderground.com/api/" + apiKey + "/history_" + dateString + "/q/pws:" + stationID +".json";
        
        URL url = new URL(apiCallString);
        
        URLConnection uc = url.openConnection();
        BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));

        String inputLine;
        StringBuilder sb = new StringBuilder();

        while ((inputLine = reader.readLine()) != null) {
            sb.append(inputLine);
        }

        JSONObject jo = new JSONObject(sb.toString());
        
        WeatherRecordConstructor wrc = new WeatherRecordConstructor();
        
        try {
            Document newRecord = WeatherRecordConstructor.constructNewRecord(jo, stationID);
            
            //Get Search query
            int year = Integer.parseInt(dateString.substring(0, 4));
            int month = Integer.parseInt(dateString.substring(4, 6));
            int date = Integer.parseInt(dateString.substring(6));
            
            
            QueryBuilder searchQuery = constructSearchQuery(stationID, year, month, date);

            weatherData.replaceOne((Bson) searchQuery.get(), newRecord, (new UpdateOptions()).upsert(true));      

        } catch (NumberFormatException nfe) {
            
        }

        return true;
    }
    
    //@param month zero indexed
    private QueryBuilder constructSearchQuery(String stationID, int year, int month, int day) {
                
        Document innerDate = new Document();
        innerDate.put("year", year);
        innerDate.put("month", month);
        innerDate.put("day", day);
        
        QueryBuilder finalQuery = QueryBuilder.start().put("id").is(stationID);        
        finalQuery.put("date").is(innerDate);

        return finalQuery;
    }
}
