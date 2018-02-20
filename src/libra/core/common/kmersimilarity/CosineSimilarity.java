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
package libra.core.common.kmersimilarity;

import java.io.IOException;
import libra.core.common.WeightAlgorithm;
import libra.preprocess.common.kmerstatistics.KmerStatistics;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class CosineSimilarity extends AbstractScore {
    private static final Log LOG = LogFactory.getLog(CosineSimilarity.class);
    
    private double[] param_array;
    
    public CosineSimilarity() {
    }
    
    @Override
    public void setParam(int size, double[] param_array) {
        if(this.param_array == null) {
            this.param_array = new double[size];
        }
        
        for(int i=0;i<size;i++) {
            this.param_array[i] = param_array[i];
        }
    }
    
    @Override
    public void setParam(int size, WeightAlgorithm algorithm, KmerStatistics[] statistics) throws IOException {
        if(this.param_array == null) {
            this.param_array = new double[size];
        }
        
        switch(algorithm) {
            case LOGALITHM:
                for(int i=0;i<size;i++) {
                    this.param_array[i] = statistics[i].getLogTFCosineNormBase();
                }
                break;
            case NATURAL:
                for(int i=0;i<size;i++) {
                    this.param_array[i] = statistics[i].getNaturalTFCosineNormBase();
                }
                break;
            case BOOLEAN:
                for(int i=0;i<size;i++) {
                    this.param_array[i] = statistics[i].getBooleanTFCosineNormBase();
                }
                break;
            default:
                LOG.info("Unknown algorithm specified : " + algorithm.toString());
                throw new IOException("Unknown algorithm specified : " + algorithm.toString());
        }
    }
    
    @Override
    public void contributeScore(int size, double[] score_matrix, double[] score_array) {
        int nonZeroFields = 0;
        for(int i=0;i<size;i++) {
            if(score_array[i] != 0) {
                nonZeroFields++;
            }
        }
        
        int[] nonZeroScoresIdx = new int[nonZeroFields];
        double[] nonZeroScoresVal = new double[nonZeroFields];
        int idx = 0;
        for(int i=0;i<size;i++) {
            if(score_array[i] != 0) {
                nonZeroScoresIdx[idx] = i;
                nonZeroScoresVal[idx] = score_array[i] / this.param_array[i];
                idx++;
            }
        }
        
        for(int i=0;i<nonZeroFields;i++) {
            for(int j=0;j<nonZeroFields;j++) {
                score_matrix[nonZeroScoresIdx[i]*size + nonZeroScoresIdx[j]] += nonZeroScoresVal[i] * nonZeroScoresVal[j];
            }
        }
    }
}
