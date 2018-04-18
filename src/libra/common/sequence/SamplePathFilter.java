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

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 *
 * @author iychoi
 */
public class SamplePathFilter implements PathFilter {

    private List<PathFilter> filters;
    
    public SamplePathFilter() {
        this.filters = new ArrayList<PathFilter>();
        
        setFilters();
    }
    
    protected void setFilters() {
        this.filters.add(new FastaPathFilter());
        this.filters.add(new FastqPathFilter());
    }
    
    @Override
    public boolean accept(Path path) {
        for(PathFilter f : this.filters) {
            if(f.accept(path)) {
                return true;
            }
        }
        return false;
    }
}
