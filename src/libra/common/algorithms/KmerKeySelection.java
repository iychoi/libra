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
public class KmerKeySelection {
    public KmerKeySelection() {
        
    }
    
    public static enum KmerForm {
        FORWARD,
        REVERSE_COMPLEMENT,
    }
    
    public String selectKey(String kmer) {
        String rkmer = SequenceHelper.getReverseComplement(kmer);
            
        String kmerKey = kmer;
        if(rkmer.compareTo(kmer) < 0) {
            kmerKey = rkmer;
        }

        return kmerKey;
    }
    
    public static class KmerRecord {

        private String sequence;
        private KmerForm form;

        public KmerRecord(String sequence) {
            this.sequence = sequence;
            this.form = KmerForm.FORWARD;
        }
        
        public KmerRecord(String sequence, KmerForm form) {
            this.sequence = sequence;
            this.form = form;
        }

        public String getSequence() {
            return this.sequence;
        }

        public KmerForm getForm() {
            return this.form;
        }

        public boolean isForward() {
            return this.form == KmerForm.FORWARD;
        }

        public boolean isReverseComplement() {
            return this.form == KmerForm.REVERSE_COMPLEMENT;
        }

        public KmerRecord getReverseComplement() {
            if (this.form == KmerForm.FORWARD) {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerForm.REVERSE_COMPLEMENT);
            } else {
                return new KmerRecord(SequenceHelper.getReverseComplement(this.sequence), KmerForm.FORWARD);
            }
        }
        
        public KmerRecord getOriginalForm() {
            if (this.form == KmerForm.FORWARD) {
                return this;
            } else {
                return getReverseComplement();
            }
        }

        public KmerRecord getSelectedKey() {
            String rcSeq = SequenceHelper.getReverseComplement(this.sequence);
            if (rcSeq.compareTo(this.sequence) < 0) {
                if (this.form == KmerForm.FORWARD) {
                    return new KmerRecord(rcSeq, KmerForm.REVERSE_COMPLEMENT);
                } else {
                    return new KmerRecord(rcSeq, KmerForm.FORWARD);
                }
            } else {
                return this;
            }
        }
    }

}
