/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.collection.weatherlink;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;
import sarai.aws.data.processor.WeatherData;

/**
 *
 * @author monina
 */
public class WeatherLinkHandler extends DefaultHandler {
    
    private HashMap weatherDataMap;
    private WeatherData weatherData;
    
    private String element = "";
    private StringBuilder sb;

    public WeatherLinkHandler() {
        this.weatherDataMap = new HashMap<>();
        this.weatherData = new WeatherData();
    }
    
    @Override
    public void startDocument () {
	System.out.println("Start document");
    }

    @Override
    public void endDocument () {
	System.out.println("End document");
    }
    
    @Override
    public void startElement (String uri, String name, String qName, Attributes atts) {
	//if ("".equals (uri))
	    //System.out.println("Start element: " + name);
            element = name;
            sb = new StringBuilder();
	//else
	   // System.out.println("Start element: {" + uri + "}" + name);
           
            if ("temp_day_high_f".equals(name)) {
               
            }
           
            else if ("temp_day_low_f".equals(name)) {
                
            }
    }
    
    @Override
    public void endElement (String uri, String name, String qName) {
//	if ("".equals (uri))
//	    System.out.println("End element: " + qName);
//	else
//	    System.out.println("End element:   {" + uri + "}" + name);

        weatherDataMap.put(element, sb.toString());
    }
    
    @Override
    public void characters (char ch[], int start, int length)
    {
	//System.out.print("Characters:    \"");
	for (int i = start; i < start + length; i++) {
	    switch (ch[i]) {
	    case '\\':
		//System.out.print("\\\\");
		break;
	    case '"':
		//System.out.print("\\\"");
		break;
	    case '\n':
		//System.out.print("\\n");
		break;
	    case '\r':
		//System.out.print("\\r");
		break;
	    case '\t':
		//System.out.print("\\t");
		break;
	    default:
		//System.out.print(ch[i]);
                sb.append(ch[i]);
		break;
	    }
	}
	//System.out.print("\"\n");
    }
    
    public HashMap getWeatherData() {
//        for (Object key : weatherDataMap.keySet()) {
//            String value = (String) weatherDataMap.get(key);
//            
//            System.out.println("[ " + key + ", " + value + "]");
//        }
        
        return weatherDataMap;
    }
    
    
}
