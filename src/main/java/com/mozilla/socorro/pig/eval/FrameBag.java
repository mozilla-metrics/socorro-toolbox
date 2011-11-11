/**
 * Copyright 2010 Mozilla Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mozilla.socorro.pig.eval;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class FrameBag extends EvalFunc<DataBag> {

    private static final Pattern framePattern = Pattern.compile("^([0-9]+)\\|([0-9]+)");
    private static final Pattern newlinePattern = Pattern.compile("\n");
    private static final Pattern pipePattern = Pattern.compile("\\|");
    
    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    public DataBag exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0) {
            return null;
        }

        DataBag db = bagFactory.newDefaultBag();
        for (String dumpline : newlinePattern.split((String)input.get(0))) {
            Matcher m = framePattern.matcher(dumpline);
            if (m.find()) {
                // TODO: validate??
                // frame_group, frame_idx, module, method, src_file, number??, hex??
                String[] splits = pipePattern.split(dumpline, -1);
                Tuple t = tupleFactory.newTuple(splits.length);
                for (int i=0; i < splits.length; i++) {
                    t.set(i, splits[i]);
                }
                if (t.size() > 0) {
                    db.add(t);
                }
            }
        }

        return db;
    }

}
