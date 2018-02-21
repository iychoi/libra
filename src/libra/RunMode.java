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
    GROUP;
    
    public static RunMode fromString(String alg) {
        try {
            RunMode wa = RunMode.valueOf(alg.trim().toUpperCase());
            return wa;
        } catch (Exception ex) {
            // fall
        }
        
        if("indexing".equalsIgnoreCase(alg.trim())) {
            return PREPROCESS;
        } else if("core".equalsIgnoreCase(alg.trim())) {
            return DISTANCEMATRIX;
        } else if("score".equalsIgnoreCase(alg.trim())) {
            return DISTANCEMATRIX;
        } else if("distance".equalsIgnoreCase(alg.trim())) {
            return DISTANCEMATRIX;
        }
        
        return PREPROCESS;
    }
    
    public static boolean isRunMode(String alg) {
        try {
            RunMode wa = RunMode.valueOf(alg.trim().toUpperCase());
            return true;
        } catch (Exception ex) {
            // fall
            return false;
        }
    }
}
