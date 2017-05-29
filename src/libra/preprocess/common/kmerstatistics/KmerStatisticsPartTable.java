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
package libra.preprocess.common.kmerstatistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import libra.common.json.JsonSerializer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsPartTable {
    
    private static final Log LOG = LogFactory.getLog(KmerStatisticsPartTable.class);
    
    private static final String HADOOP_CONFIG_KEY = "libra.preprocess.common.kmerstatistics.kmerstatisticsparttable";
    
    private String name;
    private List<KmerStatisticsPart> statisticsPart = new ArrayList<KmerStatisticsPart>();
    
    public static KmerStatisticsPartTable createInstance(File file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPartTable) serializer.fromJsonFile(file, KmerStatisticsPartTable.class);
    }
    
    public static KmerStatisticsPartTable createInstance(String json) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPartTable) serializer.fromJson(json, KmerStatisticsPartTable.class);
    }
    
    public static KmerStatisticsPartTable createInstance(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPartTable) serializer.fromJsonConfiguration(conf, HADOOP_CONFIG_KEY, KmerStatisticsPartTable.class);
    }
    
    public static KmerStatisticsPartTable createInstance(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        return (KmerStatisticsPartTable) serializer.fromJsonFile(fs, file, KmerStatisticsPartTable.class);
    }
    
    public KmerStatisticsPartTable() {
    }
    
    public KmerStatisticsPartTable(String name) {
        this.name = name;
    }
    
    @JsonProperty("name")
    public String getName() {
        return this.name;
    }
    
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }
    
    @JsonIgnore
    public int statisticsPartNum() {
        return this.statisticsPart.size();
    }
    
    @JsonProperty("statistics_part")
    public Collection<KmerStatisticsPart> getStatisticsPart() {
        return this.statisticsPart;
    }
    
    @JsonProperty("statistics_part")
    public void addStatisticsPart(Collection<KmerStatisticsPart> statisticsPart) {
        this.statisticsPart.addAll(statisticsPart);
    }
    
    @JsonIgnore
    public void addStatisticsPart(KmerStatisticsPart statisticsPart) {
        this.statisticsPart.add(statisticsPart);
    }
    
    @JsonIgnore
    public void clearStatisticsPart() {
        this.statisticsPart.clear();
    }
    
    @JsonIgnore
    public void saveTo(Configuration conf) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonConfiguration(conf, HADOOP_CONFIG_KEY, this);
    }
    
    @JsonIgnore
    public void saveTo(FileSystem fs, Path file) throws IOException {
        JsonSerializer serializer = new JsonSerializer();
        serializer.toJsonFile(fs, file, this);
    }
}
