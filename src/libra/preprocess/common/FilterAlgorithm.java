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
package libra.preprocess.common;

/**
 *
 * @author iychoi
 */
public enum FilterAlgorithm {
    NONE,
    STDDEV,
    STDDEV2;
    
    public static FilterAlgorithm fromString(String alg) {
        try {
            FilterAlgorithm fa = FilterAlgorithm.valueOf(alg.trim().toUpperCase());
            return fa;
        } catch (Exception ex) {
            // fall
        }
        
        if("n".equalsIgnoreCase(alg.trim())) {
            return NONE;
        } else if("std".equalsIgnoreCase(alg.trim())) {
            return STDDEV;
        } else if("2stddev".equalsIgnoreCase(alg.trim())) {
            return STDDEV2;
        } else if("2std".equalsIgnoreCase(alg.trim())) {
            return STDDEV2;
        }
        
        return NONE;
    }
}
