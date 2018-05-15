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
package libra.distancematrix.kmersimilarity_r;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import libra.common.hadoop.io.datatypes.IntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.helpers.MapReduceHelper;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.distancematrix.common.DistanceMatrixConfig;
import libra.distancematrix.common.DistanceMatrixConfigException;
import libra.distancematrix.common.helpers.KmerSimilarityHelper;
import libra.distancematrix.common.kmersimilarity.AbstractScore;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecord;
import libra.distancematrix.common.kmersimilarity.KmerSimilarityResultPartRecordGroup;
import libra.distancematrix.common.kmersimilarity.ScoreFactory;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.KmerIndexHelper;
import libra.preprocess.common.kmerindex.KmerIndexTable;
import libra.preprocess.common.kmerindex.KmerIndexTableRecord;
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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 *
 * @author iychoi
 */
public class KmerSimilarityReduce {
    private static final Log LOG = LogFactory.getLog(KmerSimilarityReduce.class);
    
    public KmerSimilarityReduce() {
        
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
    
    public int runJob(Configuration conf, DistanceMatrixConfig cConfig) throws Exception {
        // check config
        validateDistanceMatrixConfig(cConfig);
        
        Job job = Job.getInstance(conf, "Libra - Computing distance matrix");
        conf = job.getConfiguration();
        
        // set user configuration
        cConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerSimilarityReduce.class);
        
        // Mapper
        job.setMapperClass(KmerSimilarityMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(CompressedSequenceWritable.class);
        job.setMapOutputValueClass(IntArrayWritable.class);
        
        // Combiner
        job.setCombinerClass(KmerSimilarityCombiner.class);
        
        // Partitioner
        job.setPartitionerClass(KmerSimilarityPartitioner.class);

        // Reducer
        job.setReducerClass(KmerSimilarityReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Inputs
        List<Path> inputKmerIndexFiles = new ArrayList<Path>();
        List<Path> inputKmerIndexDataFiles = new ArrayList<Path>();
        for(FileTable fileTable : cConfig.getFileTables()) {
            String kmerIndexTableFileName = KmerIndexHelper.makeKmerIndexTableFileName(fileTable.getName());
            Path kmerIndexTableFilePath = new Path(cConfig.getKmerIndexPath(), kmerIndexTableFileName);
            inputKmerIndexFiles.add(kmerIndexTableFilePath);

            // add data
            FileSystem fs = kmerIndexTableFilePath.getFileSystem(conf);
            KmerIndexTable kmerIndexTable = KmerIndexTable.createInstance(fs, kmerIndexTableFilePath);
            for(KmerIndexTableRecord record : kmerIndexTable.getRecord()) {
                String indexDataFile = record.getIndexDataFile();
                Path kmerIndexDataFilePath = new Path(cConfig.getKmerIndexPath(), indexDataFile);
                Path kmerIndexDataFilePath_DATA = new Path(kmerIndexDataFilePath, "data");
                inputKmerIndexDataFiles.add(kmerIndexDataFilePath_DATA);
            }
        }
        
        SequenceFileInputFormat.addInputPaths(job, FileSystemHelper.makeCommaSeparated(inputKmerIndexDataFiles.toArray(new Path[0])));
        LOG.info("Input kmer index files : " + inputKmerIndexFiles.size());
        for(Path inputFile : inputKmerIndexFiles) {
            LOG.info("> " + inputFile.toString());
        }
        
        KmerMatchFileMapping fileMapping = new KmerMatchFileMapping();
        for(FileTable fileTable : cConfig.getFileTables()) {
            for(String sample : fileTable.getSamples()) {
                fileMapping.addSampleFile(sample);
            }
        }
        fileMapping.saveTo(conf);
        
        FileOutputFormat.setOutputPath(job, new Path(cConfig.getOutputPath()));
        job.setOutputFormatClass(TextOutputFormat.class);

        // Reducer
        // Use many reducers
        int reducers = conf.getInt("mapred.reduce.tasks", 1);
        if(cConfig.getTaskNum() > 0) {
            reducers = cConfig.getTaskNum();
        }
        job.setNumReduceTasks(reducers);
        LOG.info("# of Reducers : " + reducers);
        
        // Execute job and return status
        boolean result = job.waitForCompletion(true);

        // commit results
        if(result) {
            commit(new Path(cConfig.getOutputPath()), conf);
            
            // create a file mapping table file
            Path fileMappingTablePath = new Path(cConfig.getOutputPath(), KmerSimilarityHelper.makeKmerSimilarityFileMappingTableFileName());
            FileSystem fs = fileMappingTablePath.getFileSystem(conf);
            fileMapping.saveTo(fs, fileMappingTablePath);
            
            // combine results
            combineResults(fileMapping, new Path(cConfig.getOutputPath()), ScoreFactory.getScore(cConfig.getScoreAlgorithm()), conf);
        }
        
        report.addJob(job);
        
        // report
        if(cConfig.getReportPath() != null && !cConfig.getReportPath().isEmpty()) {
            report.writeTo(cConfig.getReportPath());
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
