/*
 * Copyright 2016 iychoi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package libra.common.hadoop.io.datatypes;

import java.util.Comparator;
import libra.common.helpers.SequenceHelper;

/**
 *
 * @author iychoi
 */
public class CompressedSequenceWritableComparator implements Comparator<CompressedSequenceWritable> {

    @Override
    public int compare(CompressedSequenceWritable t1, CompressedSequenceWritable t2) {
        byte[] cs_t1 = t1.getCompressedSequence();
        byte[] cs_t2 = t2.getCompressedSequence();
        
        return SequenceHelper.compareSequences(cs_t1, cs_t2);
    }
}
