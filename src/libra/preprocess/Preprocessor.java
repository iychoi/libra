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
package libra.preprocess;

import libra.common.cmdargs.CommandArgumentsParser;
import libra.common.helpers.FileSystemHelper;
import libra.preprocess.common.FilterAlgorithm;
import libra.preprocess.common.PreprocessorConfig;
import libra.preprocess.common.PreprocessorRoundConfig;
import libra.preprocess.common.helpers.FileTableHelper;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.samplegroup.SampleGroup;
import libra.preprocess.common.samplegroup.SampleGrouper;
import libra.preprocess.stage1.KmerHistogramBuilder;
import libra.preprocess.stage2.KmerFilterBuilder;
import libra.preprocess.stage3.KmerIndexBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class Preprocessor extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(Preprocessor.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Preprocessor(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration common_conf = this.getConf();
        CommandArgumentsParser<PreprocessorCmdArgs> parser = new CommandArgumentsParser<PreprocessorCmdArgs>();
        PreprocessorCmdArgs cmdParams = new PreprocessorCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        if(cmdParams.isHelp()) {
            printHelp();
            return 1;
        }
        
        PreprocessorConfig ppConfig = cmdParams.getPreprocessorConfig();
        
        // find input files
        Path[] inputFiles = FileSystemHelper.getAllSamplePaths(common_conf, ppConfig.getSamplePath());
        
        // group samples
        SampleGrouper grouper = new SampleGrouper(ppConfig.getGroupSize(), ppConfig.getMaxGroupNum());
        SampleGroup[] groups = grouper.group(inputFiles, common_conf);
        
        // make filetables
        FileTable[] tables = new FileTable[groups.length];
        for(int i=0;i<groups.length;i++) {
            SampleGroup group = groups[i];
            
            tables[i] = new FileTable(group, ppConfig.getKmerSize());
        }
        
        int res = 0;
        try {
            for(int i=0;i<tables.length;i++) {
                // save file table
                FileTable table = tables[i];
                String fileTableFileName = FileTableHelper.makeFileTableFileName(table.getName());
                Path fileTableFile = new Path(ppConfig.getFileTablePath(), fileTableFileName);
                FileSystem outputFileSystem = fileTableFile.getFileSystem(common_conf);
                table.saveTo(outputFileSystem, fileTableFile);
            }
            
            for(int i=0;i<tables.length;i++) {
                FileTable table = tables[i];
                
                LOG.info(String.format("Processing sample files : group %d / %d, %d files", i+1, tables.length, table.samples()));
                
                PreprocessorRoundConfig roundConfig = new PreprocessorRoundConfig(ppConfig);
                roundConfig.setFileTable(table);
                
                if (!ppConfig.getSkipHistogram()) {
                    if (ppConfig.getUseHistogram()) {
                        KmerHistogramBuilder kmerHistogramBuilder = new KmerHistogramBuilder();
                        res = kmerHistogramBuilder.runJob(new Configuration(common_conf), roundConfig);
                        if(res != 0) {
                            throw new Exception("KmerHistogramBuilder Failed : " + res);
                        }
                    }
                }
                
                if(ppConfig.getFilterAlgorithm() != FilterAlgorithm.NONE &&
                        ppConfig.getFilterAlgorithm() != FilterAlgorithm.NOTUNIQUE) {
                    KmerFilterBuilder kmerFilterBuilder = new KmerFilterBuilder();
                    res = kmerFilterBuilder.runJob(new Configuration(common_conf), roundConfig);
                    if(res != 0) {
                        throw new Exception("KmerFilterBuilder Failed : " + res);
                    }
                }
                
                KmerIndexBuilder kmerIndexBuilder = new KmerIndexBuilder();
                res = kmerIndexBuilder.runJob(new Configuration(common_conf), roundConfig);
                if(res != 0) {
                    throw new Exception("KmerIndexBuilder Failed : " + res);
                }
            }
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
            res = 1;
        }
        
        return res;
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Libra : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Sample Preprocessor");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> preprocessor <arguments ...>");
    }
}
