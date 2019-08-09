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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputSplit extends InputSplit implements Writable {

    private int kmerSize;
    private Path[] kmerIndexTableFilePaths;
    private int partitionNo;

    public KmerMatchInputSplit() {    
    }
    
    public KmerMatchInputSplit(int kmerSize, Path[] kmerIndexTableFilePaths, int partitionNo) {
        this.kmerSize = kmerSize;
        this.kmerIndexTableFilePaths = kmerIndexTableFilePaths;
        this.partitionNo = partitionNo;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public Path[] getIndexTableFilePaths() {
        return this.kmerIndexTableFilePaths;
    }
    
    public int getPartitionNo() {
        return this.partitionNo;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(Path path : this.kmerIndexTableFilePaths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }
            sb.append(path.toString());
        }
        return this.partitionNo + "\n" + sb.toString();
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {"localhost"};
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerSize);
        out.writeInt(this.partitionNo);
        out.writeInt(this.kmerIndexTableFilePaths.length);
        for (Path indexPath : this.kmerIndexTableFilePaths) {
            Text.writeString(out, indexPath.toString());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.kmerSize = in.readInt();
        this.partitionNo = in.readInt();
        this.kmerIndexTableFilePaths = new Path[in.readInt()];
        for(int i=0;i<this.kmerIndexTableFilePaths.length;i++) {
            this.kmerIndexTableFilePaths[i] = new Path(Text.readString(in));
        }
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
        return Long.MAX_VALUE;
    }
}
