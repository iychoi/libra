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

/**
 *
 * @author iychoi
 */
public class KmerLines {
    private int len;
    private String[] lines;
    
    public KmerLines(int len) {
        this.len = len;
        this.lines = new String[len];
    }
    
    public void set(int idx, String line) {
        this.lines[idx] = line;
    }
    
    public String[] get() {
        return this.lines;
    }
    
    public String get(int idx) {
        return this.lines[idx];
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String s : this.lines) {
            if(s != null) {
                if(sb.length() != 0) {
                    sb.append(",");
                }
                sb.append(s);
            }
        }
        return sb.toString();
    }
}
