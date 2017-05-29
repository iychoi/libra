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
package libra.core.kmersimilarity_r;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import libra.common.hadoop.io.datatypes.CompressedIntArrayWritable;
import libra.common.hadoop.io.datatypes.CompressedSequenceWritable;
import libra.common.helpers.FileSystemHelper;
import libra.common.report.Report;
import libra.common.helpers.MapReduceHelper;
import libra.common.kmermatch.KmerMatchFileMapping;
import libra.core.commom.CoreConfig;
import libra.core.commom.CoreConfigException;
import libra.core.common.helpers.KmerSimilarityHelper;
import libra.core.common.kmersimilarity.KmerSimilarityResultPartRecord;
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
    
    private void validateCoreConfig(CoreConfig cConfig) throws CoreConfigException {
        if(cConfig.getKmerIndexPath() == null) {
            throw new CoreConfigException("cannot find input kmer index path");
        }
        
        if(cConfig.getFileTable() == null || cConfig.getFileTable().size() <= 0) {
            throw new CoreConfigException("cannot find input path");
        }
        
        if(cConfig.getKmerHistogramPath() == null) {
            throw new CoreConfigException("cannot find kmer histogram path");
        }
        
        if(cConfig.getKmerStatisticsPath() == null) {
            throw new CoreConfigException("cannot find kmer statistics path");
        }
        
        if(cConfig.getOutputPath() == null) {
            throw new CoreConfigException("cannot find output path");
        }
    }
    
    public int runJob(Configuration conf, CoreConfig cConfig) throws Exception {
        // check config
        validateCoreConfig(cConfig);
        
        Job job = Job.getInstance(conf, "Libra Core - Computing similarity");
        conf = job.getConfiguration();
        
        // set user configuration
        cConfig.saveTo(conf);
        
        Report report = new Report();
        
        job.setJarByClass(KmerSimilarityReduce.class);
        
        // Mapper
        job.setMapperClass(KmerSimilarityMapper.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapOutputKeyClass(CompressedSequenceWritable.class);
        job.setMapOutputValueClass(CompressedIntArrayWritable.class);
        
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
        for(FileTable fileTable : cConfig.getFileTable()) {
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
        for(FileTable fileTable : cConfig.getFileTable()) {
            for(String sample : fileTable.getSamples()) {
                fileMapping.addSequenceFile(sample);
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
            combineResults(new Path(cConfig.getOutputPath()), conf);
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
    
    private void combineResults(Path outputPath, Configuration conf) throws IOException {
        Path[] resultPartFiles = KmerSimilarityHelper.getKmerSimilarityResultPartFilePath(conf, outputPath);
        FileSystem fs = outputPath.getFileSystem(conf);

        KmerSimilarityResultPartRecord scoreRec = null;
        for(Path resultPartFile : resultPartFiles) {
            FSDataInputStream is = fs.open(resultPartFile);
            FileStatus status = fs.getFileStatus(resultPartFile);
            
            LineRecordReader reader = new LineRecordReader(is, 0, status.getLen(), conf);
            
            LongWritable off = new LongWritable();
            Text val = new Text();

            while(reader.next(off, val)) {
                if(scoreRec == null) {
                    scoreRec = KmerSimilarityResultPartRecord.createInstance(val.toString());
                } else {
                    KmerSimilarityResultPartRecord rec2 = KmerSimilarityResultPartRecord.createInstance(val.toString());
                    scoreRec.addScore(rec2.getScore());
                }
            }
            
            reader.close();
        }
        
        double[] accumulatedScore = scoreRec.getScore();
        
        String resultFilename = KmerSimilarityHelper.makeKmerSimilarityResultFileName();
        Path resultFilePath = new Path(outputPath, resultFilename);
        
        LOG.info("Creating a final score file : " + resultFilePath.toString());
        
        FSDataOutputStream os = fs.create(resultFilePath);
        int n = (int)Math.sqrt(accumulatedScore.length);
        for(int i=0;i<accumulatedScore.length;i++) {
            int x = i/n;
            int y = i%n;
            double score = Math.min(accumulatedScore[i], 1.0);
            
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
