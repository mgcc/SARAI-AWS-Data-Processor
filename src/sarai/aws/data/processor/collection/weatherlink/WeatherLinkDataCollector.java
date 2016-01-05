/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.collection.weatherlink;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoCollection;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.TimerTask;

import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLReaderFactory;
import org.bson.Document;
import sarai.aws.data.processor.storage.WeatherDataStorer;

/**
 *
 * @author monina
 */
public class WeatherLinkDataCollector extends TimerTask {
    public final static String TYPE = "WEATHERLINK";
    
    private String link;
    static final String OUTPUT_ENCODING = "UTF-8";
    WeatherDataStorer dataStorer;
    
    public WeatherLinkDataCollector(String link, WeatherDataStorer dataStorer) {
        this.link = link;
        this.dataStorer = dataStorer;
    }

    @Override
    public void run() {        
        try {
            //Set up needed data
            XMLReader xr = XMLReaderFactory.createXMLReader();            
            WeatherLinkHandler handler = new WeatherLinkHandler();            
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);            
            URL oracle = new URL(link);
            
            //Get weather data from online source
            InputSource is = new InputSource(new InputStreamReader(oracle.openStream()));
            xr.parse(is);
            
            //Processing??
            //@TODO: Should the handler actually be storing data? Can it be a static class that handles the elements and passes information to the collector instead?
            HashMap map = handler.getWeatherData();
            Document weatherDocument = organizeData(map);
            
            storeData(weatherDocument);
            
            
            
            
            //Store weather data in MongoDB
            //Must separate this eventually
            
            
            //handler.getWeatherData();
            System.out.println("Done");
        } catch (SAXException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        }
   
    }
    
    private Document organizeData(HashMap map) {
        //@TODO: Refactor this shit
//        BasicDBObject document = new BasicDBObject();
        
//        document.put("temp_day_high_f", map.get("temp_day_high_f"));
//        document.put("temp_day_high_time", "temp_day_high_time");
//        document.put("temp_day_low_f", map.get("temp_day_low_f"));
//        document.put("temp_day_low_time", "temp_day_low_time");

        Document document = new Document("temp_day_high_f", map.get("temp_day_high_f"))
                .append("temp_day_high_time", "temp_day_high_time")
                .append("temp_day_low_f", map.get("temp_day_low_f"))
                .append("temp_day_low_time", "temp_day_low_time");
        
        return document;
    }
    
    private void storeData(Document document) {
        dataStorer.storeWeatherDocument(document);

        System.out.println("Stored!\n");
    }
  
}
