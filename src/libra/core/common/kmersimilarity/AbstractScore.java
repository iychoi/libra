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

/**
 *
 * @author iychoi
 */
public abstract class AbstractScore {
    public abstract void setParam(int size, double[] param_array);
    public abstract void setParam(int size, WeightAlgorithm algorithm, KmerStatistics[] statistics) throws IOException;
    public abstract void contributeScore(int size, double[] score_matrix, double[] score_array);
    public abstract double accumulateScore(double score1, double score2);
    public abstract double finalizeScore(double score);
}
