/*
 * Copyright (c) 2011-2015 EPFL DATA Laboratory
 * Copyright (c) 2014-2015 The Squall Collaboration (see NOTICE)
 *
 * All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ch.epfl.data.squall.operators;

import java.util.List;
import java.util.HashSet;
import java.util.Random;

import org.apache.log4j.Logger;

import ch.epfl.data.squall.utilities.SystemParameters;
import ch.epfl.data.squall.visitors.OperatorVisitor;

public class CustomSampleOperatorReachGraph extends OneToOneOperator implements Operator {
    private static Logger LOG = Logger.getLogger(CustomSampleOperatorReachGraph.class);
    private static final long serialVersionUID = 1L;
    private int _numTuplesProcessed = 0;
    private boolean _in = false;
    private int _rate = 0;

    private HashSet<String> inDegree = new HashSet<String>();
    private HashSet<String> outDegree = new HashSet<String>();
    
    public CustomSampleOperatorReachGraph(int rate, boolean in) {
        _rate = rate;
        _in = in;
        createInDegrees();
        createOutDegrees();
        LOG.info("Custom Sample operator for Reachability Graph ");
    }

    private void createInDegrees() {
        outDegree.add("5325333");   //      "blogspot.com", 
        outDegree.add("41718572");  //      "wordpress.com",
        outDegree.add("42467638");  //      "youtube.com",
        outDegree.add("41410181");  //      "wikipedia.org",
        outDegree.add("33878319");  //      "serebella.com",
        outDegree.add("31674470");  //      "refertus.info",
        outDegree.add("38484153");  //      "top20directory.com",
        outDegree.add("39262267");  //      "typepad.com",
        outDegree.add("5686278");   //      "botw.org",
        outDegree.add("39095913");  //      "tumblr.com",
        outDegree.add("10906824");  //      "dmoz.org",
        outDegree.add("40304803");  //      "vindhetviahier.nl",
        outDegree.add("19887682");  //      "jcsearch.com",
        outDegree.add("35748264");  //      "startpagina.nl",
        outDegree.add("42206842");  //      "yahoo.com",
        outDegree.add("36992745");  //      "tatu.us",
        outDegree.add("14602869");  //      "freeseek.org",
        outDegree.add("22195621");  //      "lap.hu",
        outDegree.add("5263966");   //      "blau-webkatalog.com",
        outDegree.add("1903103");   //      "allepaginas.nl"
    }

    private void createOutDegrees() {
        inDegree.add("41718621"); // wordpress.org
        inDegree.add("42467638"); // youtube.com
        inDegree.add("41410181"); // wikipedia.org   1,243,291
        inDegree.add("15777213"); // gmpg.org    1,156,727
        inDegree.add("5325333");  // blogspot.com    1,034,450
        inDegree.add("15964788"); // google.com  782,660
        inDegree.add("41718572"); // wordpress.com   710,590
        inDegree.add("39224483"); // twitter.com 646,239
        inDegree.add("42206842"); // yahoo.com   554,251
        inDegree.add("14050903"); // flickr.com  339,231
        inDegree.add("13237914"); // facebook.com    314,051
        inDegree.add("2719708");  // apple.com   312,396
        inDegree.add("25196427"); // miibeian.gov.cn 289,605
        inDegree.add("40294265"); // vimeo.com   269,003
        inDegree.add("39095913"); // tumblr.com  226,596
        inDegree.add("20328765"); // joomla.org  201,863
        inDegree.add("2150098"); // amazon.com  196,690
        inDegree.add("40673739"); // w3.org  196,507
        inDegree.add("27729888"); // nytimes.com 193,907
        inDegree.add("35243431"); // sourceforge.net 189,663//
    }

    @Override
    public void accept(OperatorVisitor ov) {
	ov.visit(this);
    }

    @Override
    public List<String> getContent() {
	throw new RuntimeException(
		"getContent for CustomSampleOperatorReachGraph should never be invoked!");
    }

    @Override
    public int getNumTuplesProcessed() {
	return _numTuplesProcessed;
    }

    @Override
    public boolean isBlocking() {
	return false;
    }

    @Override
    public String printContent() {
	throw new RuntimeException(
		"printContent for CustomSampleOperatorReachGraph should never be invoked!");
    }

    @Override
    public List<String> processOne(List<String> tuple, long lineageTimestamp) {
        _numTuplesProcessed++;
	   
        if (_in && inDegree.contains(tuple.get(1)))
            return tuple;
        else if (!_in && outDegree.contains(tuple.get(0)))
            return tuple;
        // else if (_numTuplesProcessed % _rate == 0)
        //     return tuple;
        else
            return null;
    }

    @Override
    public String toString() {
	final StringBuilder sb = new StringBuilder();
	sb.append("CustomSampleOperatorReachGraph with rate : ");
    sb.append(_rate);
	return sb.toString();
    }
}
