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
package libra.common.sequence;

import libra.common.helpers.PathHelper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class FastaPathFilter implements PathFilter {

    private static final String[] FASTA_EXT = {"fa", "ffn", "fna", "faa", "fasta", "fas", "fsa", "seq"};
    
    @Override
    public boolean accept(Path path) {
        String ext = PathHelper.getExtensionWithoutCompressed(path.getName());
        if(ext != null) {
            ext = ext.toLowerCase();
        }
        
        for(String fext : FASTA_EXT) {
            if(fext.equals(ext)) {
                return true;
            }
        }
        
        return false;
    }
}
