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
package libra.core.kmersimilarity_r;


import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityPartitioner extends Partitioner<CompressedSequenceWritable, IntArrayWritable> {

    private static final Log LOG = LogFactory.getLog(KmerSimilarityPartitioner.class);
    
    @Override
    public int getPartition(CompressedSequenceWritable key, IntArrayWritable value, int numReduceTasks) {
        return (key.getSequence().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
