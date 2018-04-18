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
package libra.group;

import java.util.Collection;
import libra.common.cmdargs.CommandArgumentsParser;
import libra.common.helpers.FileSystemHelper;
import libra.group.common.GroupConfig;
import libra.preprocess.common.samplegroup.SampleGroup;
import libra.preprocess.common.samplegroup.SampleGrouper;
import libra.preprocess.common.samplegroup.SampleInfo;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author iychoi
 */
public class Group extends Configured implements Tool {
    private static final Log LOG = LogFactory.getLog(Group.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new Group(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration common_conf = this.getConf();
        CommandArgumentsParser<GroupCmdArgs> parser = new CommandArgumentsParser<GroupCmdArgs>();
        GroupCmdArgs cmdParams = new GroupCmdArgs();
        if(!parser.parse(args, cmdParams)) {
            LOG.error("Failed to parse command line arguments!");
            return 1;
        }
        
        if(cmdParams.isHelp()) {
            printHelp();
            return 1;
        }
        
        GroupConfig gConfig = cmdParams.getGroupConfig();
        
        // find input files
        Path[] inputFiles = FileSystemHelper.getAllSamplePaths(common_conf, gConfig.getSamplePaths());
        
        // group samples
        SampleGrouper grouper = new SampleGrouper(gConfig.getGroupSize(), gConfig.getMaxGroupNum());
        SampleGroup[] groups = grouper.group(inputFiles, common_conf);
        
        try {
            LOG.info(String.format("Total sample files : %d files", inputFiles.length));
            LOG.info(String.format("Total groups : %d groups", groups.length));
            
            for(int i=0;i<groups.length;i++) {
                SampleGroup group = groups[i];
                LOG.info(String.format("Group %d : %d files", i+1, group.samples()));
                Collection<SampleInfo> samples = group.getSamples();
                for(SampleInfo sample : samples) {
                    LOG.info(String.format("G%d> %s, size=%d", i+1, sample.getPath(), sample.getSize()));
                }
            }
        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
        }
        
        return 0;
    }

    private static void printHelp() {
        System.out.println("============================================================");
        System.out.println("Libra : Massive Comparative Analytic Tools for Metagenomics");
        System.out.println("Group");
        System.out.println("============================================================");
        System.out.println("Usage :");
        System.out.println("> group <arguments ...>");
    }
}
