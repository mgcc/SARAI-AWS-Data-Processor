/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.QueryBuilder;
import com.mongodb.client.FindIterable;
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
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author monina
 */
public class MaintenanceTask extends TimerTask{
    
    int YEAR_CURRENT;
    int MONTH_CURRENT;
    int DAY_CURRENT;
    
    MongoClient mongoClient;
    MongoDatabase db;
    
    MongoCollection awsCollection;
    MongoCollection weatherData;
    MongoCollection settings;
    
    String host;
    int port;
    String dbName;
    
    private void init() {
        mongoClient = new MongoClient(host, port); //port should be in args
        db = mongoClient.getDatabase(dbName);
        
        awsCollection = db.getCollection("weather-stations");
        weatherData = db.getCollection("weather-data");
        settings = db.getCollection("dss-settings");
    }
    
    public MaintenanceTask(String host, int port, String dbName) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
    }
    
    @Override
    public void run() {
        System.out.println("Updating...");
        
        init();
        
        try {
            updateWeatherData();
        } catch (IOException ex) {
            Logger.getLogger(MaintenanceTask.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            mongoClient.close();
        }
        
    }
    
    private void updateWeatherData() throws MalformedURLException, IOException {        
        //Get Weather Stations
        FindIterable<Document> weatherStations = awsCollection.find();
        
        //Get API Key
        Document wundergroundAPIKey = (Document) settings.find(eq("name", "wunderground-api-key")).first();
        String apiKey = wundergroundAPIKey.getString("value");
        
        //Get Last Updated
        Document lastUpdate = (Document) settings.find(eq("name", "weather-last-update")).first();
        Document lastUpdateValue = (Document) lastUpdate.get("value");
        
        System.out.println("Data was last updated on " + apiFormatDate(lastUpdateValue));
        
        //Get current date
        YEAR_CURRENT = Calendar.getInstance().get(Calendar.YEAR);
        MONTH_CURRENT = Calendar.getInstance().get(Calendar.MONTH); //already zero indexed
        DAY_CURRENT = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
        
        System.out.println("Current: " + YEAR_CURRENT + " " + MONTH_CURRENT + " " + DAY_CURRENT);
        
        int count = 0;
        int totalCalls = 0;
        
        //Get through all the weather stations by date
        for (Document cd = getNextDate(lastUpdateValue); !isEqualToCurrentDate(cd); cd = getNextDate(cd)) {
            
            for (Document station : weatherStations) {
                
                String apiCallString = "http://api.wunderground.com/api/" + apiKey + "/history_" + apiFormatDate(cd) + "/q/pws:" + station.get("id") +".json";
                
                URL url = new URL(apiCallString);
        
                URLConnection uc = url.openConnection();
                BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream()));
        
                String inputLine;
                StringBuilder sb = new StringBuilder();
   
                while ((inputLine = reader.readLine()) != null) {
                    sb.append(inputLine);
                }

                JSONObject jo = new JSONObject(sb.toString());
                
                try {
                    Document newRecord = constructNewRecord(jo, station.getString("id"));
                
                    //Get Search query
                    QueryBuilder searchQuery = constructSearchQuery(station.getString("id"), (int) cd.get("year"), (int) cd.get("month"), (int) cd.get("day"));

                    weatherData.replaceOne((Bson) searchQuery.get(), newRecord, (new UpdateOptions()).upsert(true));      
                    
                    System.out.println(++totalCalls + ": Updated " + station.get("id") + " (" + apiFormatDate(cd) +")");
                } catch (NumberFormatException nfe) {
                    //TODO: REPLACE WITH EMPTY ENTRY
                    System.out.println(++totalCalls + ": MISSING DATA " + station.get("id") + " (" + apiFormatDate(cd) +")");
                }
                              
                
                //Can't call more than 10 times per minute if key is free
                if (++count == 10) {
                    System.out.println("Sleeping for a minute...");
                    try {
                        Thread.sleep(60000);
                        System.out.println("Resuming activity...");
                        count = 0;
                    } catch (InterruptedException ex) {
                        Logger.getLogger(SARAIAWSDataProcessor.class.getName()).log(Level.SEVERE, null, ex);
                    }
                    
                }
                
            }

            //update lastUpdated in settings
            Document updatedDate = new Document(); 
            updatedDate.put("year", (int) cd.get("year"));
            updatedDate.put("month", (int) cd.get("month"));
            updatedDate.put("day", (int) cd.get("day"));
            
            BasicDBObject updatedSetting = new BasicDBObject("$set", new BasicDBObject().append("value", updatedDate));
            
            QueryBuilder wlu = QueryBuilder.start().put("name").is("weather-last-update");
            
            settings.updateMany((Bson) wlu.get(), updatedSetting);
                
        }
        
        //don't forget to print warnings on API limits
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
    
    private Document constructNewRecord(JSONObject jo, String stationID) throws NumberFormatException {
        
        //ASSEMBLE NEW RECORD
        JSONObject history = (JSONObject) jo.get("history");
        JSONArray dailySummaries = (JSONArray) history.getJSONArray("dailysummary");
                
        JSONObject dailySummary = dailySummaries.getJSONObject(0);
        JSONArray observations = history.getJSONArray("observations");
        
        Document newRecord = new Document();
        newRecord.put("name", "utc");
        newRecord.put("id", stationID);
        
        //date
        Document nrDate = new Document();
        
        JSONObject date = (JSONObject) dailySummary.get("date");
        
        Calendar c = new GregorianCalendar(
                    Integer.parseInt((String) (date.get("year"))),
                    Integer.parseInt((String) date.get("mon")) - 1,
                    Integer.parseInt((String) date.get("mday")),
                    0, 0, 0);
        
        newRecord.put("dateUTC", c.getTime());
        
        nrDate.put("year", Integer.parseInt((String) (date.get("year"))));
        nrDate.put("month", Integer.parseInt((String) date.get("mon")) - 1);
        nrDate.put("day", Integer.parseInt((String) date.get("mday")));     
        newRecord.put("date", nrDate);
        
        //temp
        Document nrTemp = new Document();
        nrTemp.put("ave", getValue(dailySummary, "meantempm", 100.0));
        nrTemp.put("max", getValue(dailySummary, "maxtempm", 100.0));
        nrTemp.put("min", getValue(dailySummary, "mintempm", 100.0));
        
       
        //pressure
        Document nrPressure = new Document();
        nrPressure.put("min", getValue(dailySummary, "minpressurem", 100.0));
        nrPressure.put("max", getValue(dailySummary, "maxpressurem", 100.0));
//        nrPressure.put("min", Float.parseFloat(dailySummary.getString("minpressurem")));
//        nrPressure.put("max", Float.parseFloat(dailySummary.getString("maxpressurem")));
        
        //wind
        Document nrWind = new Document();
        nrWind.put("maxSpd", getValue(dailySummary, "maxwspdm", 100.0));
        nrWind.put("aveSpd", getValue(dailySummary, "meanwindspdm", 100.0));
//        nrWind.put("maxSpd", Float.parseFloat(dailySummary.getString("maxwspdm")));
//        nrWind.put("aveSpd", Float.parseFloat(dailySummary.getString("meanwindspdm")));
        
        //humidity
        Document nrHumidity = new Document();
        nrHumidity.put("ave", Integer.parseInt(dailySummary.getString("humidity")));
        nrHumidity.put("min", Integer.parseInt(dailySummary.getString("minhumidity")));
        nrHumidity.put("max", Integer.parseInt(dailySummary.getString("maxhumidity")));
        
        //solar radiation
//        Document nrSolarRadiation = new Document();
//        
//        int count = 0;
//        double runningTotal = 0;
//        double max = 0;
//        
//        for (Object e : observations) {
//            JSONObject entry = (JSONObject) e;
//            double current = entry.getDouble("UV");
//            
//            if (current >= 1) {
//                double converted = entry.getDouble("UV") * 3.6;
//                runningTotal += converted;
//
//                count++;
//            }
//            
//            if ((current * 3.6) > max) { max = (current * 3.6); }
//        }
//        
//        runningTotal = runningTotal / count;
//        
//        nrSolarRadiation.put("max", max);
//        nrSolarRadiation.put("ave", runningTotal);
        
        
        //Data
        Document nrData = new Document();
        nrData.put("temp", nrTemp);
        nrData.put("pressure", nrPressure);
        nrData.put("wind", nrWind);
        nrData.put("humidity", nrHumidity);
//        nrData.put("solarRadiation", nrSolarRadiation);
        nrData.put("rainfall", getValue(dailySummary, "precipm", 100.0));
        
        
        newRecord.put("data", nrData);
        
        return newRecord;
    }
    
    private String apiFormatDate(Document cd) {
        String result = "";
        int year = (int) cd.get("year");
        int month = ((int) cd.get("month")) + 1;
        int day = (int) cd.get("day");
        
        result += year;
        result += String.format("%02d", month);
        result += String.format("%02d", day);
        
        return result;
    }
    
    private double getValue(JSONObject source, String key, double denominator) {         
        return Math.round(Float.parseFloat(source.getString(key)) * denominator) / denominator;
    }
    
    //Time to break
    private boolean isEqualToCurrentDate(Document d) {
        if (d == null) return true;
        
        if ((int) d.get("year") == YEAR_CURRENT
                && (int) d.get("month") == MONTH_CURRENT
                && (int) d.get("day") == DAY_CURRENT) {
            return true;
        } else {
            return false;
        }
    }
    
    private Document getNextDate(Document lastDate) {
        Document nextDate = new Document();
        
        int y = (int) lastDate.get("year");
        int m = (int) lastDate.get("month");
        int d = (int) lastDate.get("day");
                           
        if (d == 28 && m == 1) {
            d = 1;
            m ++;
        } else if (d == 30 & (m == 0 || m == 2 || m == 4 || m == 6 || m == 7 || m == 9 || m == 11)) {
            //has 31
            d = 31;
        } else {
            if (d == 31) {
                if (m == 11) {
                    y++;
                    m = 0;
                }
                else {
                    d = 1;
                    m++;
                }
            }
            else {
                d++;
            }
        }
        
        nextDate.put("year", y);
        nextDate.put("month", m);
        nextDate.put("day", d);
        
        return nextDate;
    }
    
    
    
}
