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

import org.xml.sax.XMLReader;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.helpers.DefaultHandler;

/**
 *
 * @author monina
 */
public class WeatherLinkDataCollector implements Runnable {
    public final static String TYPE = "WEATHERLINK";
    
    private int interval = 10000; //scrape interval in miliseconds
    private String link;
    
    static final String outputEncoding = "UTF-8";
    
    public WeatherLinkDataCollector(String link, int interval) {
        //this.interval = interval;
        this.link = link;    
    }

    @Override
    public void run() {
//        while(true) {
//            System.out.println("Running");
//            try {    
//                Thread.sleep(interval);
//            } catch (InterruptedException ex) {
//                Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
        
        try {
            XMLReader xr = XMLReaderFactory.createXMLReader();
            
            WeatherLinkHandler handler = new WeatherLinkHandler();
            
            xr.setContentHandler(handler);
            xr.setErrorHandler(handler);
            
            URL oracle = new URL("http://www.weatherlink.com/xml.php?user=uplbsaraivp1&pass=uplbs4r41vp1");
            
            InputSource is = new InputSource(new InputStreamReader(oracle.openStream()));
            
            xr.parse(is);
            
            handler.getWeatherData();
        } catch (SAXException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(WeatherLinkDataCollector.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        

	
//        FileReader r = new FileReader(args[i]);
//        xr.parse(new InputSource(r));
	

        
    }
    
    
}
