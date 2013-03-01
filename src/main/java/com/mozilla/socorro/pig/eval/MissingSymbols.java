/**
 * Copyright 2013 Mozilla Foundation <http://www.mozilla.org>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mozilla.socorro.pig.eval;

import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MissingSymbols extends EvalFunc<DataBag>
{
    private static final Pattern newlinePattern = Pattern.compile("\n");
    private static final Pattern pipePattern = Pattern.compile("\\|");

    private static final BagFactory bagFactory = BagFactory.getInstance();
    private static final TupleFactory tupleFactory = TupleFactory.getInstance();

    private class ModuleData
    {
        public String pdbname_;
        public String id_;
        public boolean reported_;

        public ModuleData(String pdbname, String id) {
            pdbname_ = pdbname;
            id_ = id;
            reported_ = false;
        }
    }    

    public DataBag exec(Tuple input) throws IOException
    {
        if (input == null || input.size() != 1) {
            return null;
        }

        if (!(input.get(0) instanceof String)) {
            return null;
        }

        String dump = (String) input.get(0);

        DataBag db = bagFactory.newDefaultBag();

        // maps "<module>" to false (not reported) or true (reported)
        HashMap modulemap = new HashMap(100);

        for (String dumpline : newlinePattern.split(dump)) {
            String[] splits = pipePattern.split(dumpline, -1);
            if (splits.length == 0 || splits[0].length() == 0) {
                continue;
            }

            if (splits[0].equals("Module")) {
                if (!modulemap.containsKey(splits[1])) {
                    modulemap.put(splits[1],
                                  new ModuleData(splits[3], splits[4]));
                }
                continue;
            }

            if (!Character.isDigit(splits[0].charAt(0)) ||
                splits.length != 7) {
                continue;
            }

            if (splits[3].length() == 0) {
                // This DLL doesn't have symbols, at least in this particular
                // location
                ModuleData md = (ModuleData) modulemap.get(splits[2]);
                if (md != null) {
                    if (!md.reported_) {
                        md.reported_ = true;

                        Tuple t = tupleFactory.newTuple(2);
                        t.set(0, md.pdbname_);
                        t.set(1, md.id_);
                        db.add(t);
                    }
                }
            }
        }
        return db;
    }

    public Schema outputSchema(Schema input) {
        try {
            Schema bagSchema = new Schema();
            bagSchema.add(
                new Schema.FieldSchema("pdbname", DataType.CHARARRAY));
            bagSchema.add(
                new Schema.FieldSchema("id", DataType.CHARARRAY));
            
            return new Schema(
                new Schema.FieldSchema("modules", bagSchema, DataType.BAG));
        }
        catch (Exception e) {
            return null;
        }
    }
}
