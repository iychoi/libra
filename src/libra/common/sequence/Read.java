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
package libra.common.sequence;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class Read {
    
    private static final Log LOG = LogFactory.getLog(Read.class);
    
    public static final char FASTA_READ_DESCRIPTION_IDENTIFIER = '>';
    public static final char FASTQ_READ_DESCRIPTION_IDENTIFIER = '@';
    public static final char FASTQ_READ_DESCRIPTION2_IDENTIFIER = '+';
    
    private String description;
    private String quality;
    private String description2;
    private List<String> sequences = new ArrayList<String>();
    
    public Read() {
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public String getDescription() {
        return this.description;
    }
    
    public void setQuality(String quality) {
        this.quality = quality;
    }
    
    public String getQuality() {
        return this.quality;
    }
    
    public void setDescription2(String description2) {
        this.description2 = description2;
    }
    
    public String getDescription2() {
        return this.description2;
    }
    
    public void addSequence(String sequence) {
        this.sequences.add(sequence);
    }
    
    public List<String> getSequences() {
        return Collections.unmodifiableList(sequences);
    }
    
    public String getFullSequence() {
        StringBuilder sb = new StringBuilder();
        for(String sequence : this.sequences) {
            sb.append(sequence.trim());
        }
        return sb.toString();
    }
    
    public void parseFasta(List<String> lines) throws IOException {
        clear();
        
        if(lines.size() < 2) {
            throw new IOException("invalid fasta read format");
        }
        
        int lineNo = 0;
        //LOG.info("Parsing a FASTA read");
        for(String line : lines) {
            //LOG.info(line);
            if(line.length() > 0) {
                if(lineNo == 0) {
                    if(line.charAt(0) == FASTA_READ_DESCRIPTION_IDENTIFIER) {
                        setDescription(line.trim());
                    } else {
                        throw new IOException("invalid fasta read description format - " + line);
                    }
                } else {
                    if(line.charAt(0) == FASTA_READ_DESCRIPTION_IDENTIFIER) {
                        throw new IOException("invalid fasta read sequence format - " + line);
                    } else {
                        addSequence(line.trim());
                    }
                }
            }
            
            lineNo++;
        }
    }
    
    public void parseFastq(List<String> lines) throws IOException {
        clear();
        
        if(lines.size() == 0) {
            return;
        }
        
        if(lines.size() != 4) {
            String header = lines.get(0);
            throw new IOException(String.format("invalid fastq read format - a read (%s) has %d lines", header, lines.size()));
        }
        
        int lineNo = 0;
        //LOG.info("Parsing a FASTQ read");
        for(String line : lines) {
            //LOG.info(line);
            switch(lineNo) {
                case 0:
                    {
                        if(line.charAt(0) == FASTQ_READ_DESCRIPTION_IDENTIFIER) {
                            setDescription(line.trim());
                        } else {
                            throw new IOException("invalid fastq read description format - " + line);
                        }
                    }
                    break;
                case 1:
                    {
                        addSequence(line.trim());
                    }
                    break;
                case 2:
                    {
                        if(line.charAt(0) == FASTQ_READ_DESCRIPTION2_IDENTIFIER) {
                            setDescription2(line.trim());
                        } else {
                            throw new IOException("invalid fastq read description2 format - " + line);
                        }
                    }
                    break;
                case 3:
                    {
                        setQuality(line.trim());
                    }
                    break;
            }
            
            lineNo++;
        }
    }
    
    public void parse(List<String> lines) throws IOException {
        // check first line
        String first = lines.get(0);
        if(first.charAt(0) == FASTQ_READ_DESCRIPTION_IDENTIFIER) {
            parseFastq(lines);
            return;
        } else if(first.charAt(0) == FASTA_READ_DESCRIPTION_IDENTIFIER) {
            parseFasta(lines);
            return;
        }
           
        throw new IOException("invalid read format");
    }
    
    public void clear() {
        this.description = null;
        this.quality = null;
        this.description2 = null;
        this.sequences.clear();
    }
    
    public boolean isEmpty() {
        return this.description == null;
    }
}
