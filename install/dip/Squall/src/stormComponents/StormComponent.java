/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package stormComponents;

import java.util.List;

public interface StormComponent {

    //Component which sends data further down
    public static final int INTERMEDIATE=1;
    //Last level component (can be part of Joiner), which does not propagate data anywhere
    public static final int FINAL_COMPONENT=2;

    public int getID();
    public String getInfoID();

    public void printTuple(List<String> tuple);
    public void printContent();

}
