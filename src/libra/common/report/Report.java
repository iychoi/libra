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
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author iychoi
 */
public class Report {
    private List<Job> jobs;
    
    public Report() {
        this.jobs = new ArrayList<Job>();
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
        String jobName = job.getJobName();
        String jobID = job.getJobID().toString();
        String jobStatus;
        try {
            jobStatus = job.getJobState().name();
        } catch (IOException ex) {
            jobStatus = "Unknown";
        } catch (InterruptedException ex) {
            jobStatus = "Unknown";
        }
        
        String startTimeStr;
        try {
            startTimeStr = TimeHelper.getTimeString(job.getStartTime());
        } catch (Exception ex) {
            startTimeStr = "Unknown";
        }
        
        String finishTimeStr;
        try {
            finishTimeStr = TimeHelper.getTimeString(job.getFinishTime());
        } catch (Exception ex) {
            finishTimeStr = "Unknown";
        }
        
        String timeTakenStr;
        try {
            timeTakenStr = TimeHelper.getDiffTimeString(job.getStartTime(), job.getFinishTime());
        } catch (Exception ex) {
            timeTakenStr = "Unknown";
        }
        
        String countersStr;
        try {
            countersStr = job.getCounters().toString();
        } catch (Exception ex) {
            countersStr = "Unknown";
        }
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "StartTime : " + startTimeStr + "\n" +
                "FinishTime : " + finishTimeStr + "\n" + 
                "TimeTaken : " + timeTakenStr + "\n\n" +
                countersStr;
    }
}
