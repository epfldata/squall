/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import java.util.Timer;
import java.util.TimerTask;
import stormComponents.StormComponent;


public class PeriodicBatchSend extends Timer {

    private PeriodicTask _pt;
    private StormComponent _comp;

    public PeriodicBatchSend(long period, StormComponent comp){
        _comp = comp;
        _pt = new PeriodicTask(comp);

        scheduleAtFixedRate(_pt, 0, period);
    }

    public StormComponent getComponent(){
        return _comp;
    }
    
    public class PeriodicTask extends TimerTask{
        private StormComponent _comp;

        public PeriodicTask(StormComponent comp){
            _comp = comp;
        }

        public void run(){
            _comp.batchSend();
        }
    }

}
