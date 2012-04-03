/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package conversion;


public class StringConversion implements TypeConversion<String>{
    private static final long serialVersionUID = 1L;

    @Override
    public String fromString(String str) {
        return str;
    }

    @Override
    public String toString(String obj) {
        return obj;
    }

    @Override
    public String getInitialValue() {
        return "";
    }
}