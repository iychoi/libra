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
package libra.distancematrix.kmersimilarity_m;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.helpers.MapReduceHelper;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.common.kmermatch.KmerMatchInputFormat;
import libra.common.kmermatch.KmerMatchInputFormatConfig;
import libra.distancematrix.common.DistanceMatrixConfig;
import libra.distancematrix.common.DistanceMatrixConfigException;
import libra.distancematrix.common.helpers.KmerSimilarityHelper;
import libra.distancematrix.common.kmersimilarity.AbstractScore;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecord;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecordGroup;
import libra.distancematrix.common.kmersimilarity.ScoreFactory;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.FileTableHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.LineRecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityMap {
    private static final Log LOG = LogFactory.getLog(KmerSimilarityMap.class);
    
    private static final int DEFAULT_INPUT_SPLITS = 100;
    
    public KmerSimilarityMap() {
        
    }
    
    private void validateDistanceMatrixConfig(DistanceMatrixConfig dmConfig) throws DistanceMatrixConfigException {
        if(dmConfig.getKmerIndexPath() == null) {
            throw new DistanceMatrixConfigException("cannot find input kmer index path");
        }
        
        if(dmConfig.getFileTables() == null || dmConfig.getFileTables().size() <= 0) {
            throw new DistanceMatrixConfigException("cannot find input path");
        }
        
        if(dmConfig.getUseHistogram()) {
            if(dmConfig.getKmerHistogramPath() == null) {
                throw new DistanceMatrixConfigException("cannot find kmer histogram path");
            }
        }
        
        if(dmConfig.getKmerStatisticsPath() == null) {
            throw new DistanceMatrixConfigException("cannot find kmer statistics path");
        }
        
        if(dmConfig.getOutputPath() == null) {
            throw new DistanceMatrixConfigException("cannot find output path");
        }
    }
    
    public int runJob(Configuration conf, DistanceMatrixConfig dmConfig) throws Exception {
        // check config
        validateDistanceMatrixConfig(dmConfig);
        
        Job job = Job.getInstance(conf, "Libra - Computing distance matrix");
        conf = job.getConfiguration();
        
        // set user configuration
        dmConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerSimilarityMap.class);
        
        // Mapper
        job.setMapperClass(KmerSimilarityMapper.class);
        job.setInputFormatClass(KmerMatchInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Inputs
        List<Path> inputFileTableFiles = new ArrayList<Path>();
        for(FileTable fileTable : dmConfig.getFileTables()) {
            String fileTableFileName = FileTableHelper.makeFileTableFileName(fileTable.getName());
            Path fileTableFilePath = new Path(dmConfig.getFileTablePath(), fileTableFileName);
            inputFileTableFiles.add(fileTableFilePath);
        }
        
        KmerMatchInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputFileTableFiles.toArray(new Path[0])));

        LOG.info("Input file table files : " + inputFileTableFiles.size());
        for(Path inputFile : inputFileTableFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        KmerMatchFileMapping fileMapping = new KmerMatchFileMapping();
        for(FileTable fileTable : dmConfig.getFileTables()) {
            for(String sample : fileTable.getSamples()) {
                fileMapping.addSampleFile(sample);
            }
        }
        fileMapping.saveTo(conf);
        
        int kmerSize = 0;
        Iterator<FileTable> iterator = dmConfig.getFileTables().iterator();
        if(iterator.hasNext()) {
            FileTable tbl = iterator.next();
            kmerSize = tbl.getKmerSize();
        }
        
        int tasks = DEFAULT_INPUT_SPLITS;
        if(dmConfig.getTaskNum() > 0) {
            tasks = dmConfig.getTaskNum();
        }
        
        KmerMatchInputFormatConfig matchInputFormatConfig = new KmerMatchInputFormatConfig();
        matchInputFormatConfig.setKmerSize(kmerSize);
        matchInputFormatConfig.setPartitionNum(tasks);
        matchInputFormatConfig.setFileTablePath(dmConfig.getFileTablePath());
        matchInputFormatConfig.setKmerIndexPath(dmConfig.getKmerIndexPath());
        if(dmConfig.getUseHistogram()) {
            matchInputFormatConfig.setUseHistogram(true);
            matchInputFormatConfig.setKmerHistogramPath(dmConfig.getKmerHistogramPath());
        } else {
            matchInputFormatConfig.setUseHistogram(false);
        }
        
        KmerMatchInputFormat.setInputFormatConfig(job, matchInputFormatConfig);
        
        FileOutputFormat.setOutputPath(job, new Path(dmConfig.getOutputPath()));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Reducer
        job.setNumReduceTasks(0);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(dmConfig.getOutputPath()), conf);
            
            // create a file mapping table file
            Path fileMappingTablePath = new Path(dmConfig.getOutputPath(), KmerSimilarityHelper.makeKmerSimilarityFileMappingTableFileName());
            FileSystem fs = fileMappingTablePath.getFileSystem(conf);
            fileMapping.saveTo(fs, fileMappingTablePath);
            
            // combine results
            combineResults(fileMapping, new Path(dmConfig.getOutputPath()), ScoreFactory.getScore(dmConfig.getScoreAlgorithm()), conf);
        }
        
        report.addJob(job);
        
        // report
        if(dmConfig.getReportPath() != null && !dmConfig.getReportPath().isEmpty()) {
            report.writeTo(dmConfig.getReportPath());
        }
        
        return result ? 0 : 1;
    }
    
    private void commit(Path outputPath, Configuration conf) throws IOException {
        FileSystem fs = outputPath.getFileSystem(conf);
        
        FileStatus status = fs.getFileStatus(outputPath);
        if (status.isDirectory()) {
            FileStatus[] entries = fs.listStatus(outputPath);
            for (FileStatus entry : entries) {
                Path entryPath = entry.getPath();
                
                // remove unnecessary outputs
                if(MapReduceHelper.isLogFiles(entryPath)) {
                    fs.delete(entryPath, true);
                } else if(MapReduceHelper.isPartialOutputFiles(entryPath)) {
                    // rename outputs
                    int mapreduceID = MapReduceHelper.getMapReduceID(entryPath);
                    String newName = KmerSimilarityHelper.makeKmerSimilarityResultPartFileName(mapreduceID);
                    Path toPath = new Path(entryPath.getParent(), newName);

                    LOG.info(String.format("rename %s ==> %s", entryPath.toString(), toPath.toString()));
                    fs.rename(entryPath, toPath);
                } else {
                    // let it be
                }
            }
        } else {
            throw new IOException("path not found : " + outputPath.toString());
        }
    }
    
    private void combineResults(KmerMatchFileMapping fileMapping, Path outputPath, AbstractScore scoreFunction, Configuration conf) throws IOException {
        int valuesLen = fileMapping.getSize();
        
        double[] accumulatedScore = new double[valuesLen * valuesLen];
        for(int i=0;i<accumulatedScore.length;i++) {
            accumulatedScore[i] = 0;
        }
        
        Path[] resultPartFiles = KmerSimilarityHelper.getKmerSimilarityResultPartFilePath(conf, outputPath);
        FileSystem fs = outputPath.getFileSystem(conf);
        
        for(Path resultPartFile : resultPartFiles) {
            LOG.info("Combining a score file : " + resultPartFile.toString());
            
            FSDataInputStream is = fs.open(resultPartFile);
            FileStatus status = fs.getFileStatus(resultPartFile);
            
            LineRecordReader reader = new LineRecordReader(is, 0, status.getLen(), conf);
            
            LongWritable off = new LongWritable();
            Text val = new Text();

            while(reader.next(off, val)) {
                KmerSimilarityResultPartRecordGroup group = KmerSimilarityResultPartRecordGroup.createInstance(val.toString());
                for(KmerSimilarityResultPartRecord scoreRec : group.getScore()) {
                    int file1ID = scoreRec.getFile1ID();
                    int file2ID = scoreRec.getFile2ID();
                    double score = scoreRec.getScore();

                    accumulatedScore[file1ID*valuesLen + file2ID] = scoreFunction.accumulateScore(accumulatedScore[file1ID*valuesLen + file2ID], score);
                }
            }
            
            reader.close();
        }
        
        String resultFilename = KmerSimilarityHelper.makeKmerSimilarityResultFileName();
        Path resultFilePath = new Path(outputPath, resultFilename);
        
        LOG.info("Creating a final score file : " + resultFilePath.toString());
        
        FSDataOutputStream os = fs.create(resultFilePath);
        int n = (int)Math.sqrt(accumulatedScore.length);
        for(int i=0;i<accumulatedScore.length;i++) {
            int x = i/n;
            int y = i%n;
            double score = scoreFunction.finalizeScore(accumulatedScore[i]);
            
            String k = x + "\t" + y;
            String v = Double.toString(score);
            String out = k + "\t" + v + "\n";
            os.write(out.getBytes());
        }
        
        os.close();
        
        for(Path resultPartFile : resultPartFiles) {
            fs.delete(resultPartFile, true);
        }
    }
}
