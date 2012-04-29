package utilities;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.io.IOException;

public class SerializableFileInputStream extends    InputStream
                                         implements Serializable, CustomReader
{
  protected final File          _file;           // The _file to read from
  protected       byte[]        _buffer;         // The _file _buffer
  protected       long          _filePtr    = 0; // How many bytes already read
                                                // from the _file
  // _filePtr might also represent the logical start of file read
  protected       int           _bufferPtr  = 0; // How many bytes into the
                                                // _buffer the user has read
  protected       int           _bufferSize = 0; // How many bytes of the _buffer
                                                // are being used
  protected       boolean       _eofReached = false;

  //Modified by Aleksandar
  // All the sections (except the first one) should omit firstLine characters.
  //   The responsability of the previous section is to finish the previous line.
  private         boolean       _omitFirstLine;
  //Logical end of the file based on (section, parts)
  private         long          _fileEndPtr;
  //Accurate position into the file (updatet after each readLine method)
  private         long          _filePosition;


  protected static int DEFAULT_BUFFER_SIZE = 1*1024*1024;
  protected static int LOW_WATER_MARK      = 1024;

  
  public SerializableFileInputStream(String file)
     throws IOException
  {
    this(new File(file), DEFAULT_BUFFER_SIZE, 0, 1);
  }
  
  public SerializableFileInputStream(File file)
     throws IOException
  {
    this(file, DEFAULT_BUFFER_SIZE, 0, 1);
  }

  public SerializableFileInputStream(File file, int bufferSize)
     throws IOException
  {
    this(file, bufferSize, 0, 1);
  }

  public SerializableFileInputStream(File file, int bufferSize, int section, int parts)
     throws IOException
  {
    _file       = file;
    _buffer     = new byte[bufferSize];
    setParameters(section, parts);
    fillBuffer();
  }

        //Modified by Aleksandar
  private void setParameters(int section, int parts){
      if(section>=parts){
          throw new RuntimeException("The section can take value from 0 to " + (parts-1));
      }

      long fileSize = _file.length();
      long sectionSize = fileSize/parts;
      _filePtr = section * sectionSize;
      _filePosition = _filePtr;

      //for all the sections except the last one, the end is sectionSize far from the beginning
      if(section == parts-1){
          _fileEndPtr = fileSize;
      }else{
        _fileEndPtr = _filePtr + sectionSize;
      }

      //for all the sections except the first one, we discard the first read line
      if(section == 0){
          _omitFirstLine = false;
      }else{
          _omitFirstLine = true;
      }
  }
  
  public int read() throws IOException {
    if(eof()){ return -1; }
    fillBufferIfNeeded(1);

    int ret = (int)_buffer[_bufferPtr];
    _bufferPtr ++;

    return ret;
  }

  @Override
  public int read(byte[] b, int off, int len) 
      throws IOException 
  {
    if(eof()) return -1;
    fillBufferIfNeeded(len);
    
    if(len > _buffer.length - _bufferSize){ len = _buffer.length - _bufferSize; }
    System.arraycopy(b, off, _buffer, _bufferPtr, len);
    _bufferPtr += len;
    
    return len;
  }
  
  public String readLine() 
      throws IOException
  {
    if(eof()){ return null; }

    int i = numberOfBytesToEOL(false);
    String ret = null;

    if((i < 0) && (_bufferPtr < _bufferSize)){
      i = _bufferSize - _bufferPtr;
    }
    if(i > 0){
      int newlineChomp = 1;
      if(_buffer[_bufferPtr+i-1] == '\n'){
        if((i > 1)&&(_buffer[_bufferPtr+i-2] == '\r')){ newlineChomp++; }
      }

      //Modified by Aleksandar
      //For the first line in a section:
      //  we neglect the line we are currently in (it might be only a part of a line),
      //  and send the next line.
      if(!_omitFirstLine){
        ret = new String(_buffer, _bufferPtr, i-newlineChomp);
        _bufferPtr += i;
        _filePosition += i;
      }else{
        _bufferPtr += i;
        _filePosition +=i;
        _omitFirstLine = false;
        return readLine();
      }
    }
    
    return ret;
  }

  protected int numberOfBytesToEOL(boolean canFail)
      throws IOException
  {
    int i;
    for(i = 0; i < _bufferSize-_bufferPtr; i++){
      if(_buffer[i+_bufferPtr] == '\n'){ break; }
      if(_buffer[i+_bufferPtr] == '\r'){
        if(i + 1 < _bufferSize){
          fillBuffer();
        }
        if((i + 1 < _bufferSize-_bufferPtr) && (_buffer[i+1+_bufferPtr] == '\n')){
          i += 1;
        }
        break;
      }
    } 
    if(i >= _bufferSize-_bufferPtr){
      if(canFail){ 
        return -1;
      } else {
        fillBuffer();
        return numberOfBytesToEOL(true);
      }
    }
    return i+1;
  }
  
  public boolean eof()
  {

    return (_eofReached && (_bufferPtr >= _bufferSize))
                  //Modified by Aleksandar
            // if _fileEnd points to a first character after \n,
            // we still want the previous section to read it
            || (_filePosition > _fileEndPtr);
  }
  
  public void close()
  {
    //ignore
  }
  
  protected void fillBufferIfNeeded(int bytesRequested) 
      throws IOException 
  {
    int currentBufferBytes = _bufferSize - _bufferPtr;
    if((currentBufferBytes < LOW_WATER_MARK) &&
       (bytesRequested > currentBufferBytes)) {
      fillBuffer();
    }
  }
  
  protected void fillBuffer() 
      throws IOException
  {
    if(_eofReached) { return; }
    
    if(_bufferPtr < 0){
      throw new IOException("Invalid Buffer Pointer: " + _bufferPtr);
    }
    
    if(_bufferPtr > 0){
      // Do some housekeeping on our _buffer
      if(_bufferPtr < _bufferSize){
        // Move any existing data to the front of the _buffer...
        // A circular _buffer would be faster, but we're really only talking
        // about a few dozen bytes at a time.
        
        // Note that System.arraycopy is explicitly safe for self-to-self copies.
        System.arraycopy(_buffer, _bufferPtr, _buffer, 0, _bufferSize-_bufferPtr);
        _bufferSize = _bufferSize - _bufferPtr;
      } else {
        // If we've precisely exhausted our _buffer (_bufferPtr should never be >
        // _bufferSize), then don't bother copying;
        _bufferSize = 0;
      }
      _bufferPtr = 0;
    }
    
    FileInputStream fis = new FileInputStream(_file);
    int bytesRead = -100;
    try {
      if(_filePtr > 0) {
        for(long i = 0; i < _filePtr; i += fis.skip(_filePtr - i)){
        }
      }
      
      bytesRead = fis.read(_buffer, _bufferSize, _buffer.length - _bufferSize);
      
      if(bytesRead < 0){
        _eofReached = true;
      } else {
        _bufferSize += bytesRead;
        _filePtr    += bytesRead;
      }
    } finally {
      fis.close();
    }
    
    //System.err.println("==== Filling : "+stats()+"; read="+bytesRead+" ====");
  }
  
  protected String stats(){
    return "bP="+_bufferPtr+"; bS="+_bufferSize+"; fP="+_filePtr;
  }
  
  //self-test
  public static void main(String args[]) 
      throws IOException
  {
    SerializableFileInputStream reader =
      new SerializableFileInputStream(args[0]);
    while(true){
      String l = reader.readLine();
      if(l == null) { break; }
      System.out.println(l);
    }
  }
}