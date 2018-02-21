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
package libra.distancematrix.common.kmersimilarity;

import java.io.IOException;
import libra.distancematrix.common.ScoreAlgorithm;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class ScoreFactory {
    private static final Log LOG = LogFactory.getLog(ScoreFactory.class);
    
    public static AbstractScore getScore(ScoreAlgorithm algorithm) throws IOException {
        switch(algorithm) {
            case COSINESIMILARITY:
                return new CosineSimilarity();
            case BRAYCURTIS:
                return new BrayCurtis();
            case JENSENSHANNON:
                return new JensenShannon();
            default:
                LOG.info("Unknown score algorithm specified : " + algorithm.toString());
                throw new IOException("Unknown score algorithm specified : " + algorithm.toString());
        }
    }
}
