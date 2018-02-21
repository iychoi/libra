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
package libra.distancematrix.common;

/**
 *
 * @author iychoi
 */
public enum ScoreAlgorithm {
    COSINESIMILARITY,
    BRAYCURTIS,
    JENSENSHANNON;
    
    public static ScoreAlgorithm fromString(String alg) {
        try {
            ScoreAlgorithm wa = ScoreAlgorithm.valueOf(alg.trim().toUpperCase());
            return wa;
        } catch (Exception ex) {
            // fall
        }
        
        if("cosine".equalsIgnoreCase(alg.trim())) {
            return COSINESIMILARITY;
        } else if("cossim".equalsIgnoreCase(alg.trim())) {
            return COSINESIMILARITY;
        } else if("cos".equalsIgnoreCase(alg.trim())) {
            return COSINESIMILARITY;
        } else if("cs".equalsIgnoreCase(alg.trim())) {
            return COSINESIMILARITY;
        } else if ("bc".equalsIgnoreCase(alg.trim())) {
            return BRAYCURTIS;
        } else if ("bray".equalsIgnoreCase(alg.trim())) {
            return BRAYCURTIS;
        } else if ("jensen".equalsIgnoreCase(alg.trim())) {
            return JENSENSHANNON;
        } else if ("jensha".equalsIgnoreCase(alg.trim())) {
            return JENSENSHANNON;
        } else if ("js".equalsIgnoreCase(alg.trim())) {
            return JENSENSHANNON;
        }
        
        return COSINESIMILARITY;
    }
}
