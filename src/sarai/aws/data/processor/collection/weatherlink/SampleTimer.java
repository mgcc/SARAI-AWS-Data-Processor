/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sarai.aws.data.processor.collection.weatherlink;

import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

/**
 *
 * @author monina
 */
public class SampleTimer {
    Timer timer;
    
    public SampleTimer(String name, int seconds) {
        timer = new Timer();
        timer.scheduleAtFixedRate(new RemindTask(name), new Date(), seconds * 1000);
    }

  class RemindTask extends TimerTask {
      String name;
      public RemindTask(String name) {
          this.name = name;
      }
    public void run() {
	
        	System.out.println("Task " + name + " firing");
		
    }
  }

  public static void main(String args[]) {
    System.out.println("About to schedule tasks..");
    
    new SampleTimer("first", 2);
    new SampleTimer("second", 5);
    
    
    System.out.println("Task scheduled.");
  }

    
}//first first second first first first-second