/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.constructor;

import java.util.Calendar;
import java.util.GregorianCalendar;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 *
 * @author monina
 */
public class WeatherRecordConstructor {
    
    public WeatherRecordConstructor() {
        
    }
    
    public static Document constructNewRecord(JSONObject jo, String stationID) throws NumberFormatException {
        
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
    
    private static double getValue(JSONObject source, String key, double denominator) {         
        return Math.round(Float.parseFloat(source.getString(key)) * denominator) / denominator;
    }
}
