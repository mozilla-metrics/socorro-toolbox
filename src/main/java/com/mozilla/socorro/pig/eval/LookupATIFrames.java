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
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaUtil;

public class LookupATIFrames extends EvalFunc<Integer>
{
    private static final Pattern newlinePattern = Pattern.compile("\n");
    private static final Pattern pipePattern = Pattern.compile("\\|");

    /* returns 0==notpresent 1==present 2==topofstack */
    public Integer exec(Tuple input) throws IOException
    {
        if (input == null || input.size() != 1) {
            return null;
        }

        if (!(input.get(0) instanceof String)) {
            return null;
        }

        String dump = (String) input.get(0);

        boolean found = false;

        for (String dumpline : newlinePattern.split(dump)) {
            String[] splits = pipePattern.split(dumpline, -1);
            if (splits.length == 0 || splits[0].length() == 0 ||
                !Character.isDigit(splits[0].charAt(0))) {
                continue;
            }

            // threadno, frameno, module, function, srcfile, line, offset
            if (splits.length != 7) {
                continue;
            }

            String module = splits[2];
            if (module.equals("atiumdag.dll")) {
                int fno = Integer.parseInt(splits[1]);
                if (fno == 0) {
                    return 2;
                }
                found = true;
            }
        }
        if (found) {
            return 1;
        }
        return 0;
    }
}
