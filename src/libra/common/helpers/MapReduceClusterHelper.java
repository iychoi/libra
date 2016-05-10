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
package libra.common.helpers;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.ClusterStatus;
import org.apache.hadoop.mapred.JobClient;

/**
 *
 * @author iychoi
 */
public class MapReduceClusterHelper {
    
    private static final Log LOG = LogFactory.getLog(MapReduceClusterHelper.class);
    
    public static int getNodeNum(Configuration conf) {
        try {
            JobClient client = new JobClient(conf);
            ClusterStatus clusterStatus = client.getClusterStatus();
            return clusterStatus.getTaskTrackers();
        } catch (IOException ex) {
            LOG.error(ex);
            return -1;
        }
    }
}
