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
package libra.distancematrix;

import libra.common.cmdargs.CommandArgumentsParser;
import libra.distancematrix.common.DistanceMatrixConfig;
import libra.distancematrix.common.RunMode;
import libra.distancematrix.kmersimilarity_m.KmerSimilarityMap;
import libra.distancematrix.kmersimilarity_r.KmerSimilarityReduce;
import libra.preprocess.common.filetable.FileTable;
import libra.preprocess.common.helpers.FileTableHelper;
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
public class DistanceMatrix extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(DistanceMatrix.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DistanceMatrix(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration common_conf = this.getConf();
        CommandArgumentsParser<DistanceMatrixCmdArgs> parser = new CommandArgumentsParser<DistanceMatrixCmdArgs>();
        DistanceMatrixCmdArgs cmdParams = new DistanceMatrixCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        if(cmdParams.isHelp()) {
            printHelp();
            return 1;
        }
        
        DistanceMatrixConfig dmConfig = cmdParams.getDistanceMatrixConfig();
        
        // find file tables
        Path fileTablePath = new Path(dmConfig.getFileTablePath());
        Path[] fileTableFiles = FileTableHelper.getFileTableFilePath(common_conf, fileTablePath);
        
        // load file tables
        for(Path fileTableFile : fileTableFiles) {
            FileSystem fs = fileTableFile.getFileSystem(common_conf);
            FileTable fileTable = FileTable.createInstance(fs, fileTableFile);
            dmConfig.addFileTable(fileTable);
        }
        
        int res = 0;
        try {
            if(dmConfig.getRunMode() == RunMode.MAP) {
                KmerSimilarityMap kmerSimilarityMap = new KmerSimilarityMap();
                res = kmerSimilarityMap.runJob(new Configuration(common_conf), dmConfig);
                if(res != 0) {
                    throw new Exception("KmerSimilarityMap Failed : " + res);
                }
            } else if(dmConfig.getRunMode() == RunMode.REDUCE) {
                KmerSimilarityReduce kmerSimilarityReduce = new KmerSimilarityReduce();
                res = kmerSimilarityReduce.runJob(new Configuration(common_conf), dmConfig);
                if(res != 0) {
                    throw new Exception("KmerSimilarityReduce Failed : " + res);
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
        System.out.println("Distance Matrix Computation");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> distancematrix <arguments ...>");
    }
}
