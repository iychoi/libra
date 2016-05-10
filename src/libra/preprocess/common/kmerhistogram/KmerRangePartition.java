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
package libra.preprocess.common.kmerhistogram;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;
import libra.common.helpers.SequenceHelper;
import org.apache.hadoop.io.Text;

/**
 *
 * @author iychoi
 */
public class KmerRangePartition {
    private int kmerSize;
    private int numPartitions;
    private int partitionIndex;
    private BigInteger partitionSize;
    private BigInteger partitionBegin;
    private BigInteger parititionEnd;
    
    public KmerRangePartition() {
    }
    
    public KmerRangePartition(int kmerSize, int numPartition, int partitionIndex, BigInteger partitionSize, BigInteger partitionBegin, BigInteger partitionEnd) {
        this.kmerSize = kmerSize;
        this.numPartitions = numPartition;
        this.partitionIndex = partitionIndex;
        this.partitionSize = partitionSize;
        this.partitionBegin = partitionBegin;
        this.parititionEnd = partitionEnd;
    }
    
    public int getKmerSize() {
        return this.kmerSize;
    }
    
    public int getNumPartitions() {
        return this.numPartitions;
    }
    
    public int getPartitionIndex() {
        return this.partitionIndex;
    }
    
    public BigInteger getPartitionSize() {
        return this.partitionSize;
    }
    
    public BigInteger getPartitionBegin() {
        return this.partitionBegin;
    }
    
    public String getPartitionBeginKmer() {
        return SequenceHelper.convertToString(this.partitionBegin, this.kmerSize);
    }
    
    public BigInteger getPartitionEnd() {
        return this.parititionEnd;
    }
    
    public String getPartitionEndKmer() {
        return SequenceHelper.convertToString(this.parititionEnd, this.kmerSize);
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.kmerSize);
        out.writeInt(this.numPartitions);
        out.writeInt(this.partitionIndex);
        Text.writeString(out, this.partitionSize.toString());
        Text.writeString(out, this.partitionBegin.toString());
        Text.writeString(out, this.parititionEnd.toString());
    }

    public void read(DataInput in) throws IOException {
        this.kmerSize = in.readInt();
        this.numPartitions = in.readInt();
        this.partitionIndex = in.readInt();
        this.partitionSize = new BigInteger(Text.readString(in));
        this.partitionBegin = new BigInteger(Text.readString(in));
        this.parititionEnd = new BigInteger(Text.readString(in));
    }
    
    @Override
    public String toString() {
        return "kmerSize : " + this.kmerSize + ", numPartition : " + this.numPartitions + ", partitionIndex : " + this.partitionIndex +
                ", partitionSize : " + this.partitionSize.toString() + ", beginKmer : " + SequenceHelper.convertToString(this.partitionBegin, this.kmerSize) + ", endKmer : " + SequenceHelper.convertToString(this.parititionEnd, this.kmerSize);
    }
}
