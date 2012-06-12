/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package schema;

import conversion.TypeConversion;
import java.io.Serializable;

public class ColumnNameType implements Serializable {
    private static final long serialVersionUID = 1L;

    private String _name;
    private TypeConversion _type;

    public ColumnNameType(String name, TypeConversion type){
        _name = name;
        _type = type;
    }

    public String getName(){
        return _name;
    }

    public TypeConversion getType(){
        return _type;
    }
}