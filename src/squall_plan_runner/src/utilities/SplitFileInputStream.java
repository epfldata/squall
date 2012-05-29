/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import org.apache.log4j.Logger;

/*
 * BufferedReader gives no information about number of lines read for readLine
 *   by standard, line might be ended by \n, \r, or \r\n
 * FileInputStream cannot offer readLine method.
 *
 * To solve this problem, we can use the following solutions
 * 1) SerializableFileInputStream,
 *      the behavior implemented is the same as in BufferedReader.
 * 2) Hope a user uses only \n as an 'end of line' character.
 *      do everything using BufferedReader.
 *    No performance improvement expected. NO GENERAL ENOUGH.
 *    This class uses BufferedReader as explained. Earlier it was not possible,
 *      but it seems that Storm fixed the bug in the meantime.
 * 3)We could split by the number of lines as well,
 *   but then we must count the lines before splitting.
 * The code for counting the lines is here:
 *   LineNumberReader  lnr = new LineNumberReader(new FileReader(new File("File1")));
 *   lnr.skip(Long.MAX_VALUE);
 *   System.out.println(lnr.getLineNumber());
 * Probably worst performance.
 */

public class SplitFileInputStream implements Serializable, CustomReader {
    private static final long serialVersionUID = 1L;
    private static Logger LOG = Logger.getLogger(SplitFileInputStream.class);

    private DataInputStream _in;
    private BufferedReader _reader;

    private boolean _omitFirstLine;

    private long _filePosition;
    private long _fileEndPtr;

    public SplitFileInputStream(String path, int section, int parts){
        setParameters(path, section, parts);
    }

    public String readLine() throws IOException{
        if(eof()){
            return null;
        }

        String line = _reader.readLine();
        int length=0;
        if(line != null){
            length = line.length();
        }
         _filePosition += length + 1; // // + 1 for \n character

        if (_omitFirstLine){
            _omitFirstLine = false;
            return readLine();
        }else{
            return line;
        }
    }

    public void close(){
        try {
            _in.close();
            _reader.close();
        } catch (IOException ex) {
            LOG.info(MyUtilities.getStackTrace(ex));
        }
    }

    private void setParameters(String path, int section, int parts){
        if(section>=parts){
            throw new RuntimeException("The section can take value from 0 to " + (parts-1));
        }

        File file = new File(path); // no close method for this class
        long fileSize = file.length();
        long sectionSize = fileSize/parts;
        _filePosition = section * sectionSize;
        openFileSection(path, _filePosition);

        //for all the sections except the last one, the end is sectionSize far from the beginning
        if(section == parts-1){
            _fileEndPtr = fileSize;
        }else{
            _fileEndPtr = _filePosition + sectionSize;
        }

        //for all the sections except the first one, we discard the first read line
        if(section == 0){
            _omitFirstLine = false;
        }else{
            _omitFirstLine = true;
        }
    }

    private void openFileSection(String path, long fileBeginning){
        FileInputStream fstream = null;
        try {
            fstream = new FileInputStream(path);
            _in = new DataInputStream(fstream);
            _reader = new BufferedReader(new InputStreamReader(_in));
            if (fileBeginning > 0) {
                for (long i = 0; i < fileBeginning; i += _reader.skip(fileBeginning - i)) {
                }
            }
        } catch (IOException ex) {
            String msg = MyUtilities.getStackTrace(ex);
            LOG.info(msg);
            throw new RuntimeException(msg);
        }
    }

    private boolean eof(){
        return _filePosition > _fileEndPtr;
    }

    public static void main(String[] args){

        String path = args[0];
        int section = Integer.parseInt(args[1]);
        int parts = Integer.parseInt(args[2]);
        SplitFileInputStream reader = new SplitFileInputStream(path, section, parts);

        try {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException ex) {
            LOG.info(MyUtilities.getStackTrace(ex));
        }
    }
}
