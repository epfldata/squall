/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package conversion;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import utilities.MyUtilities;

import org.apache.log4j.Logger;

public class DateConversion implements TypeConversion<Date>{
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(DateConversion.class);

    private static String DATE_FORMAT = "yyyy-MM-dd";
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

}