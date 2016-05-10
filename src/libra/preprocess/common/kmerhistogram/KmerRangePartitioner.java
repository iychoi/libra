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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.helpers.SequenceHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class KmerRangePartitioner {
    
    private static final Log LOG = LogFactory.getLog(KmerRangePartitioner.class);
    
    private int kmerSize;
    private int numPartitions;

    public KmerRangePartitioner(int kmerSize, int numPartitions) {
        this.kmerSize = kmerSize;
        this.numPartitions = numPartitions;
    }
    
    public KmerRangePartition[] getHistogramPartitions(Collection<KmerHistogramRecord> records, long samples) {
        return getHistogramPartitions(records.toArray(new KmerHistogramRecord[0]), samples);
    }
    
    public KmerRangePartition[] getHistogramPartitions(KmerHistogramRecord[] records, long samples) {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        String As = "";
        String Ts = "";
        for(int i=0;i<this.kmerSize;i++) {
            As += "A";
            Ts += "T";
        }
        
        long partitionWidth = samples / this.numPartitions;
        long partitionWidthRemain = partitionWidth;
        int partitionIdx = 0;
        int recordIdx = 0;
        long curRecordRemain = records[0].getFrequency();
        BigInteger nextPartitionBegin = BigInteger.ZERO;
        while(partitionIdx < this.numPartitions) {
            long diff = partitionWidthRemain - curRecordRemain;
            if(diff > 0) {
                partitionWidthRemain -= curRecordRemain;
                recordIdx++;
                if(recordIdx < records.length) {
                    curRecordRemain = records[recordIdx].getFrequency();
                } else {
                    break;
                }
            } else if(diff == 0) {
                BigInteger partitionBegin = nextPartitionBegin;
                BigInteger partitionEnd = null;
                if(partitionIdx == this.numPartitions - 1) {
                    partitionEnd = SequenceHelper.convertToBigInteger(Ts);
                } else {
                    partitionEnd = SequenceHelper.convertToBigInteger((records[recordIdx].getKmer() + Ts).substring(0, this.kmerSize));
                }
                BigInteger partitionSize = partitionEnd.subtract(partitionBegin);
                partitions[partitionIdx] = new KmerRangePartition(this.kmerSize, this.numPartitions, partitionIdx, partitionSize, partitionBegin, partitionEnd);
                
                nextPartitionBegin = partitionEnd.add(BigInteger.ONE);
                partitionIdx++;
                recordIdx++;
                if(recordIdx < records.length) {
                    curRecordRemain = records[recordIdx].getFrequency();
                } else {
                    break;
                }
                partitionWidthRemain = partitionWidth;
            } else {
                // in between
                BigInteger partitionBegin = nextPartitionBegin;
                BigInteger partitionEnd = null;
                if(partitionIdx == this.numPartitions - 1) {
                    partitionEnd = SequenceHelper.convertToBigInteger(Ts);
                } else {
                    BigInteger recordBegin = SequenceHelper.convertToBigInteger((records[recordIdx].getKmer() + As).substring(0, this.kmerSize));
                    BigInteger recordEnd = SequenceHelper.convertToBigInteger((records[recordIdx].getKmer() + Ts).substring(0, this.kmerSize));
                    BigInteger recordWidth = recordEnd.subtract(recordBegin);
                    BigInteger curWidth = recordWidth.multiply(BigInteger.valueOf(partitionWidthRemain)).divide(BigInteger.valueOf(records[recordIdx].getFrequency()));
                    
                    BigInteger bigger = null;
                    if(recordBegin.compareTo(partitionBegin) > 0) {
                        bigger = recordBegin;
                    } else {
                        bigger = partitionBegin;
                    }
                    
                    partitionEnd = bigger.add(curWidth);
                }
                BigInteger partitionSize = partitionEnd.subtract(partitionBegin);
                partitions[partitionIdx] = new KmerRangePartition(this.kmerSize, this.numPartitions, partitionIdx, partitionSize, partitionBegin, partitionEnd);
                
                nextPartitionBegin = partitionEnd.add(BigInteger.ONE);
                partitionIdx++;
                curRecordRemain -= partitionWidthRemain;
                partitionWidthRemain = partitionWidth;
            }
        }
        
        return partitions;
    }
    
    public KmerRangePartition[] getEqualRangePartitions() {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        
        BigInteger slice_width = kmerend.divide(BigInteger.valueOf(this.numPartitions));
        if(kmerend.mod(BigInteger.valueOf(this.numPartitions)).intValue() != 0) {
            slice_width = slice_width.add(BigInteger.ONE);
        }
        
        for(int i=0;i<this.numPartitions;i++) {
            BigInteger slice_begin = slice_width.multiply(BigInteger.valueOf(i));
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            } 

            BigInteger slice_end = slice_begin.add(slice_width).subtract(BigInteger.ONE);

            KmerRangePartition slice = new KmerRangePartition(this.kmerSize, this.numPartitions, i, slice_width, slice_begin, slice_end);
            partitions[i] = slice;
        }
        
        return partitions;
    }
    
    public KmerRangePartition[] getEqualAreaPartitions() {
        KmerRangePartition[] partitions = new KmerRangePartition[this.numPartitions];
        
        // calc 4^kmerSize
        BigInteger kmerend = BigInteger.valueOf(4).pow(this.kmerSize);
        BigDecimal bdkmerend = new BigDecimal(kmerend);
        // moves between x (0~1) y (0~1)
        // sum of area (0.5)
        double kmerArea = 0.5;
        double sliceArea = kmerArea / this.numPartitions;
        
        // we think triangle is horizontally flipped so calc get easier.
        double x1 = 0;
        
        List<BigInteger> widths = new ArrayList<BigInteger>();
        BigInteger widthSum = BigInteger.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            // x2*x2 = 2*sliceArea + x1*x1
            double temp = (2*sliceArea) + (x1*x1);
            double x2 = Math.sqrt(temp);
            
            BigDecimal bdx1 = BigDecimal.valueOf(x1);
            BigDecimal bdx2 = BigDecimal.valueOf(x2);
            
            // if i increases, bdw will be decreased
            BigDecimal bdw = bdx2.subtract(bdx1);
            
            BigInteger bw = bdw.multiply(bdkmerend).toBigInteger();
            if(bw.compareTo(BigInteger.ZERO) <= 0) {
                bw = BigInteger.ONE;
            }
            
            if(widthSum.add(bw).compareTo(kmerend) > 0) {
                bw = kmerend.subtract(widthSum);
            }
            
            if(i == this.numPartitions - 1) {
                // last case
                if(widthSum.add(bw).compareTo(kmerend) < 0) {
                    bw = kmerend.subtract(widthSum);
                }    
            }
            
            // save it
            widths.add(bw);
            widthSum = widthSum.add(bw);
            
            x1 = x2;
        }
        
        BigInteger cur_begin = BigInteger.ZERO;
        for(int i=0;i<this.numPartitions;i++) {
            BigInteger slice_width = widths.get(this.numPartitions - 1 - i);
            
            BigInteger slice_begin = cur_begin;
            
            if(slice_begin.add(slice_width).compareTo(kmerend) > 0) {
                slice_width = kmerend.subtract(slice_begin);
            }
            
            BigInteger slice_end = cur_begin.add(slice_width).subtract(BigInteger.ONE);
            
            KmerRangePartition slice = new KmerRangePartition(this.kmerSize, this.numPartitions, i, slice_width, slice_begin, slice_end);
            partitions[i] = slice;
            
            cur_begin = cur_begin.add(slice_width);
        }
        
        return partitions;
    }
}
