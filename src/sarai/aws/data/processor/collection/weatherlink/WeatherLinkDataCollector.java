/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.collection.weatherlink;

import java.io.BufferedReader;
import static java.lang.Thread.sleep;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.TimerTask;

import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author monina
 */
public class WeatherLinkDataCollector extends TimerTask {
    public final static String TYPE = "WEATHERLINK";
    
    private String link;
    
    static final String outputEncoding = "UTF-8";
    
    public WeatherLinkDataCollector(String link) {
        this.link = link; 
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
            
            //Store weather data in MongoDB
            
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
  
}
