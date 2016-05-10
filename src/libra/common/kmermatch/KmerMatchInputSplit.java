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
package libra.common.kmermatch;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import libra.preprocess.common.kmerhistogram.KmerRangePartition;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputSplit extends InputSplit implements Writable {

    private Path[] kmerIndexPath;
    private KmerRangePartition partition;

    public KmerMatchInputSplit() {    
    }
    
    public KmerMatchInputSplit(Path[] kmerIndexFilePath, KmerRangePartition partition) {
        this.kmerIndexPath = kmerIndexFilePath;
        this.partition = partition;
    }
    
    public Path[] getIndexFilePath() {
        return this.kmerIndexPath;
    }
    
    public KmerRangePartition getPartition() {
        return this.partition;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Path path : this.kmerIndexPath) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(path.toString());
        }
        return this.partition.toString() + "\n" + sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {"localhost"};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerIndexPath.length);
        for (Path indexPath : this.kmerIndexPath) {
            Text.writeString(out, indexPath.toString());
        }
        this.partition.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerIndexPath = new Path[in.readInt()];
        for(int i=0;i<this.kmerIndexPath.length;i++) {
            this.kmerIndexPath[i] = new Path(Text.readString(in));
        }
        this.partition = new KmerRangePartition();
        this.partition.read(in);
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return this.partition.getPartitionSize().longValue();
    }
}
