/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.storage;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

/**
 *
 * @author monina
 */
public class WeatherDataStorer {
    //Couldn't think of a better name OK?! :-/ Damn these java naming conventions
    
    MongoCollection weatherCollection;
    
    public WeatherDataStorer(MongoCollection weatherCollection) {
        this.weatherCollection = weatherCollection;
        
    }
    
    public void storeWeatherDocument(Document document) {
        weatherCollection.insertOne(document);
    }
}
