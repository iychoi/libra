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
package libra.common.algorithms;

import libra.common.helpers.SequenceHelper;

/**
 *
 * @author iychoi
 */
public class CanonicalKmer {
    public CanonicalKmer() {
        
    }
    
    public static enum KmerStrand {
        FORWARD,
        REVERSE_COMPLEMENT,
    }
    
    public String canonicalize(String kmer) {
        String rkmer = SequenceHelper.getReverseComplement(kmer);
            
        String kmerKey = kmer;
        if(rkmer.compareTo(kmer) < 0) {
            kmerKey = rkmer;
        }

        return kmerKey;
    }
    
    public static class KmerRecord {

        private String sequence;
        private KmerStrand strand;

        public KmerRecord(String sequence) {
            this.sequence = sequence;
            this.strand = KmerStrand.FORWARD;
        }
        
        public KmerRecord(String sequence, KmerStrand strand) {
            this.sequence = sequence;
            this.strand = strand;
        }

        public String getSequence() {
            return this.sequence;
        }

        public KmerStrand getStrand() {
            return this.strand;
        }

        public boolean isForward() {
            return this.strand == KmerStrand.FORWARD;
        }

        public boolean isReverseComplement() {
            return this.strand == KmerStrand.REVERSE_COMPLEMENT;
        }

        public KmerRecord getReverseComplement() {
            if (this.strand == KmerStrand.FORWARD) {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerStrand.REVERSE_COMPLEMENT);
            } else {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerStrand.FORWARD);
            }
        }
        
        public KmerRecord getForward() {
            if (this.strand == KmerStrand.FORWARD) {
                return this;
            } else {
                return getReverseComplement();
            }
        }

        public KmerRecord getCanonicalKmer() {
            String rcSeq = SequenceHelper.getReverseComplement(this.sequence);
            if (rcSeq.compareTo(this.sequence) < 0) {
                if (this.strand == KmerStrand.FORWARD) {
                    return new KmerRecord(rcSeq, KmerStrand.REVERSE_COMPLEMENT);
                } else {
                    return new KmerRecord(rcSeq, KmerStrand.FORWARD);
                }
            } else {
                return this;
            }
        }
    }

}
