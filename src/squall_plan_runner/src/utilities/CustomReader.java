/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package utilities;

import java.io.IOException;


public interface CustomReader {

    public String readLine()throws IOException;
    public void close();
}
