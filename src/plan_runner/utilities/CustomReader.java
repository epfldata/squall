/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package plan_runner.utilities;

import java.io.IOException;


public interface CustomReader {

    public String readLine()throws IOException;
    public void close();
}
