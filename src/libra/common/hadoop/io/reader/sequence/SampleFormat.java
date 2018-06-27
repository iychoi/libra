/*
 * Copyright 2018 iychoi.
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
package libra.common.hadoop.io.reader.sequence;

import libra.common.sequence.FastaPathFilter;
import libra.common.sequence.FastqPathFilter;
import org.apache.hadoop.fs.Path;

/**
 *
 * @author iychoi
 */
public enum SampleFormat {
    FASTA,
    FASTQ;
    
    private static FastaPathFilter fastaFilter = new FastaPathFilter();
    private static FastqPathFilter fastqFilter = new FastqPathFilter();
    
    public static SampleFormat fromString(String format) {
        try {
            SampleFormat sf = SampleFormat.valueOf(format.trim().toUpperCase());
            return sf;
        } catch (Exception ex) {
            // fall
        }
        
        return FASTA;
    }

    public static SampleFormat fromPath(Path path) {
        if(fastaFilter.accept(path)) {
            return SampleFormat.FASTA;
        }
        
        if(fastqFilter.accept(path)) {
            return SampleFormat.FASTQ;
        }
        
        return SampleFormat.FASTA;
    }
}
