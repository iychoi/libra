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
package libra.preprocess.common.kmerindex;

import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import org.apache.hadoop.io.IntWritable;

/**
 *
 * @author iychoi
 */
public class KmerIndexBufferEntry {
    private CompressedSequenceWritable key;
    private IntWritable val;

    public KmerIndexBufferEntry(CompressedSequenceWritable key, IntWritable val) {
        this.key = key;
        this.val = val;
    }

    public CompressedSequenceWritable getKey() {
        return this.key;
    }

    public IntWritable getVal() {
        return this.val;
    }
}
