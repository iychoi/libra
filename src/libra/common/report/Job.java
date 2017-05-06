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
package libra.common.report;

import java.io.IOException;

/**
 *
 * @author iychoi
 */
public class Job {
    
    private String name;
    private String id;
    private String status;
    private long startTime;
    private long finishTime;
    private String countersStr;
    
    public Job() {
    }
    
    public Job(org.apache.hadoop.mapreduce.Job job) {
        this.name = job.getJobName();
        this.id = job.getJobID().toString();
        try {
            this.status = job.getJobState().name();
        } catch (IOException ex) {
            this.status = "Unknown";
        } catch (InterruptedException ex) {
            this.status = "Unknown";
        }
        
        try {
            this.startTime = job.getStartTime();
        } catch (Exception ex) {
            this.startTime = 0;
        }
        
        try {
            this.finishTime = job.getFinishTime();
        } catch (Exception ex) {
            this.finishTime = 0;
        }
        
        try {
            this.countersStr = job.getCounters().toString();
        } catch (Exception ex) {
            this.countersStr = "Unknown";
        }
    }
    
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getID() {
        return id;
    }

    public void setID(String id) {
        this.id = id;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public String getCountersStr() {
        return countersStr;
    }

    public void setCountersStr(String countersStr) {
        this.countersStr = countersStr;
    }
}

