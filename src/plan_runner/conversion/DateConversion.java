/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.conversion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;
import plan_runner.utilities.MyUtilities;

public class DateConversion implements TypeConversion<Date>{
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DateConversion.class);

    private static final String DATE_FORMAT = "yyyy-MM-dd";

    //this cannot be static, because a static field with a constructor cannot be serialized
    private SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);

    @Override
    public Date fromString(String str) {
        Date date = null;
        try{
            date = format.parse(str);
        } catch(ParseException pe){
            String error=MyUtilities.getStackTrace(pe);
            LOG.info(error);
            throw new RuntimeException("Invalid Date Format for " + str);
        }
        return date;
    }

    @Override
    public String toString(Date obj) {
        return format.format(obj);
    }

    @Override
    public Date getInitialValue() {
        return new Date();
    }

    @Override
    public double getDistance(Date bigger, Date smaller) {
        DateTime smallerDT = new DateTime(smaller);
        DateTime biggerDT = new DateTime(bigger);
        return Days.daysBetween(smallerDT, biggerDT).getDays();
    }
    
    //for printing(debugging) purposes
    @Override
    public String toString(){
        return  "DATE";
    }

}