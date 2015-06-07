/*
 *
 *  * Copyright (c) 2011-2015 EPFL DATA Laboratory
 *  * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *  *
 *  * All rights reserved.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package ch.epfl.data.squall.dbtoaster;

import org.apache.log4j.Logger;
import scala.collection.immutable.$colon$colon;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import ddbt.lib.Messages.*;
import ddbt.lib.IQuery;
import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

public class DBToasterEngine implements Serializable {

    private static Logger LOG = Logger.getLogger(DBToasterEngine.class);

    public static final byte TUPLE_DELETE = -0x01;
    public static final byte TUPLE_INSERT = 0x01;

    private static final List EMPTY_LIST = List$.MODULE$.empty();

    private static List<Object> convertTupleToDbtTuple(java.util.List<Object> ts) {
        List<Object> result = EMPTY_LIST;
        ListIterator<Object> li = ts.listIterator(ts.size()); // start after the last element
        while (li.hasPrevious()) {
            result = new $colon$colon<Object>(li.previous(), result);
        }
        return result;
    }

    private IQuery _query; // DBToaster Query class

    public DBToasterEngine(String queryClass) {
        try {
            LOG.info("Loading Query class: " + queryClass);
            ClassLoader cl = this.getClass().getClassLoader();
            if (cl == null) cl = ClassLoader.getSystemClassLoader();
            _query = (IQuery) cl.loadClass(queryClass).newInstance();
            _query.handleEvent(new StreamInit(0)); // invoke initialization event
        } catch (Exception e) {
            throw new RuntimeException("Fail to initialize Query class " + queryClass, e);
        }
    }

    public void insertTuple(String relationName, java.util.List<Object> tuple) {
        receiveTuple(relationName, TUPLE_INSERT, tuple);
    }

    public void deleteTuple(String relationName, java.util.List<Object> tuple) {
        receiveTuple(relationName, TUPLE_DELETE, tuple);
    }

    public void receiveTuple(String relationName, byte tupleOp, java.util.List<Object> tuple) {
        List<Object> dbtTuple = convertTupleToDbtTuple(tuple);
        _query.handleEvent(new TupleEvent(tupleOp, relationName, dbtTuple));
    }

    public java.util.List<Object[]> getStreamOfUpdateTuples() {

        java.util.List<Object[]> outputTuples = new LinkedList();
        List<Object> updateStream = (List<Object>) _query.handleEvent(new GetStream(1, false));
        Iterator<Object> iterator = scala.collection.JavaConversions.asJavaIterator(updateStream.iterator());

        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (o instanceof Object[]) {
                outputTuples.add((Object[]) o);
            } else {
                outputTuples.add(new Object[] {o});
            }
        }

        return outputTuples;
    }

    public void endStream() {
        _query.handleEvent(EndOfStream$.MODULE$);
    }

}
