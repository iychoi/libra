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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import libra.common.helpers.TimeHelper;

/**
 *
 * @author iychoi
 */
public class Report {
    private List<Job> jobs;
    
    public Report() {
        this.jobs = new ArrayList<Job>();
    }

    public void addJob(org.apache.hadoop.mapreduce.Job job) {
        this.jobs.add(new Job(job));
    }
    
    public void addJob(Job job) {
        this.jobs.add(job);
    }
    
    public void addJob(Job[] jobs) {
        for(Job job : jobs) {
            this.jobs.add(job);
        }
    }
    
    public void addJob(List<Job> jobs) {
        this.jobs.addAll(jobs);
    }
    
    public void writeTo(String filename) throws IOException {
        writeTo(new File("./", filename));
    }
    
    public void writeTo(File f) throws IOException {
        if(f.getParentFile() != null) {
            if(!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
        }
        
        Writer writer = new FileWriter(f, true);
        boolean first = true;
        for(Job job : this.jobs) {
            if(first) {
                first = false;
            }
            writer.write(makeText(job));
            writer.write("\n\n");
        }
        
        writer.close();
    }
    
    private String makeText(Job job) {
        return "Job : " + job.getName() + "\n" +
                "JobID : " + job.getID() + "\n" + 
                "Status : " + job.getStatus() + "\n" +
                "StartTime : " + TimeHelper.getTimeString(job.getStartTime()) + "\n" +
                "FinishTime : " + TimeHelper.getTimeString(job.getFinishTime()) + "\n" + 
                "TimeTaken : " + TimeHelper.getDiffTimeString(job.getStartTime(), job.getFinishTime()) + "\n\n" +
                job.getCountersStr();
    }
}

