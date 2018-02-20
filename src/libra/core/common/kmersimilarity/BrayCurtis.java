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
public class BrayCurtis extends AbstractScore {
    private static final Log LOG = LogFactory.getLog(BrayCurtis.class);
    
    private double[] param_array;
    
    public BrayCurtis() {
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
                    this.param_array[i] = statistics[i].getLogTFSum();
                }
                break;
            case NATURAL:
                for(int i=0;i<size;i++) {
                    this.param_array[i] = statistics[i].getNaturalTFSum();
                }
                break;
            case BOOLEAN:
                for(int i=0;i<size;i++) {
                    this.param_array[i] = statistics[i].getBooleanTFSum();
                }
                break;
            default:
                LOG.info("Unknown algorithm specified : " + algorithm.toString());
                throw new IOException("Unknown algorithm specified : " + algorithm.toString());
        }
    }
    
    @Override
    public void contributeScore(int size, double[] score_matrix, double[] score_array) {
        for(int i=0;i<size;i++) {
            for(int j=0;j<size;j++) {
                double sum_total = this.param_array[i] + this.param_array[j];
                double diff = Math.abs(score_array[i] - score_array[j]);
                
                score_matrix[i*size + j] += diff / sum_total;
            }
        }
    }
    
    @Override
    public double accumulateScore(double score1, double score2) {
        return score1 + score2;
    }

    @Override
    public double finalizeScore(double score) {
        // compute similarity from dissimilarity
        double similarity = 1 - score;
        if(similarity < 0) {
            similarity = 0;
        }
        
        if(similarity > 1) {
            similarity = 1;
        }
        
        return similarity;
    }
}
