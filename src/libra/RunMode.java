/*
 * Copyright 2017 iychoi.
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
package libra;

/**
 *
 * @author iychoi
 */
public enum RunMode {
    PREPROCESS,
    DISTANCEMATRIX,
    GROUP,
    MERGE;
    
    private final static String PREPROCESS_MATCH_STRINGS[] = {"indexing", "index"};
    private final static String DISTANCE_MATRIX_MATCH_STRINGS[] = {"core", "score", "distance", "matrix"};
    private final static String GROUP_MATCH_STRINGS[] = {"groups"};
    private final static String MERGE_MATCH_STRINGS[] = {"merge_index"};
    
    public static RunMode fromString(String alg) {
        try {
            RunMode wa = RunMode.valueOf(alg.trim().toUpperCase());
            return wa;
        } catch (Exception ex) {
            // fall
        }
        
        // compare to a list of match strings and return a run mode
        
        // preprocess match strings
        for(String match : PREPROCESS_MATCH_STRINGS) {
            if(match.equalsIgnoreCase(alg.trim())) {
                return PREPROCESS;
            }
        }
        
        // distance matrix match strings
        for(String match : DISTANCE_MATRIX_MATCH_STRINGS) {
            if(match.equalsIgnoreCase(alg.trim())) {
                return DISTANCEMATRIX;
            }
        }
        
        // group match strings
        for(String match : GROUP_MATCH_STRINGS) {
            if(match.equalsIgnoreCase(alg.trim())) {
                return GROUP;
            }
        }
        
        // merge match strings
        for(String match : MERGE_MATCH_STRINGS) {
            if(match.equalsIgnoreCase(alg.trim())) {
                return MERGE;
            }
        }
        
        return null;
    }
}
