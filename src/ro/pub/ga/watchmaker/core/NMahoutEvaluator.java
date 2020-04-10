/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ro.pub.ga.watchmaker.core;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringUtils;
import org.apache.mahout.ga.watchmaker.EvalMapper;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;

import com.google.common.io.Closeables;
import com.google.gson.reflect.TypeToken;

/**
 * Generic Mahout distributed evaluator. takes an evaluator and a population and launches a Hadoop job. The
 * job evaluates the fitness of each individual of the population using the given evaluator. Takes care of
 * storing the population into an input file, and loading the fitness from job outputs.
 */
public class NMahoutEvaluator<T> {

  public NMahoutEvaluator() {
  }
  
  /**
   * Uses Mahout to evaluate every candidate from the input population using the given evaluator.
   *
   * @param evaluator
   *          FitnessEvaluator to use
   * @param population
   *          input population
   * @param evaluations
   *          {@code List<Double>} that contains the evaluated fitness for each candidate from the
   *          input population, sorted in the same order as the candidates.
   */
  public void evaluate(FitnessEvaluator<?> evaluator,
                              Iterable<?> population,
                              int slaveNumber,
                              int populationSize,
                              Collection<EvaluatedCandidateWritable<T>> evaluations,
                              Path input,
                              Path output) throws IOException, ClassNotFoundException, InterruptedException {

    Job job = Job.getInstance();
    job.setJarByClass(NMahoutEvaluator.class);

    Configuration conf = job.getConfiguration();

    FileSystem fs = FileSystem.get(conf);
    HadoopUtil.delete(conf, input);
    HadoopUtil.delete(conf, output);
    
    storePopulation(fs, new Path(input, "population"), population);
    
    configureJob(job, conf, evaluator, populationSize / (2 * slaveNumber) ,input, output);
    
    job.waitForCompletion(true);
    
    NOutputUtils<T> outputUtils = new NOutputUtils<T>();
    
    outputUtils.importEvaluations(fs, conf, output, evaluations);

  }

  /**
   * Configure the job
   *
   * @param evaluator
   *          FitnessEvaluator passed to the mapper
   * @param inpath
   *          input {@code Path}
   * @param outpath
   *          output {@code Path}
   */
  private void configureJob(Job job,
                                   Configuration conf,
                                   FitnessEvaluator<?> evaluator,
                                   int linesPerMap,
                                   Path inpath,
                                   Path outpath) {

    conf.set("mapreduce.input.fileinputformat.inputdir", inpath.toString());
    conf.set("mapreduce.output.fileoutputformat.outputdir", outpath.toString());
    
    //Set he N value for the NLineInputFormat
    conf.setInt("mapreduce.input.lineinputformat.linespermap", linesPerMap);
    
    TypeToken<EvaluatedCandidateWritable<T>> evalCandidateTypeToken = new TypeToken<EvaluatedCandidateWritable<T>>() {};
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(evalCandidateTypeToken.getRawType());
    
    TypeToken<DistributedFitnessMapper<T>> mapperTypeToken = new TypeToken<DistributedFitnessMapper<T>>() {};
    TypeToken<DistributedFitnessCombiner<T>> combinerTypeToken = new TypeToken<DistributedFitnessCombiner<T>>() {};
    
    job.setMapperClass((Class<? extends Mapper>) mapperTypeToken.getRawType());
    job.setCombinerClass((Class<? extends Reducer>)combinerTypeToken.getRawType());
    
   // job.setInputFormatClass(TextInputFormat.class);
    job.setInputFormatClass(NLineInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
   
    
    // store the stringified evaluator
    conf.set(EvalMapper.MAHOUT_GA_EVALUATOR, StringUtils.toString(evaluator));
  }
  
  /**
   * Stores a population of candidates in the output file path.
   * 
   * @param fs
   *          FileSystem used to create the output file
   * @param f
   *          output file path
   * @param population
   *          population to store
   */
  static void storePopulation(FileSystem fs, Path f, Iterable<?> population) throws IOException {
	
//	if(!fs.exists(f)) {
//		
//		fs.createNewFile(f);
//	}
	
    FSDataOutputStream out = fs.create(f, true);
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(out));
    
    try {
      for (Object candidate : population) {
        writer.write(StringUtils.toString(candidate));
        writer.newLine();
      }
    } finally {
      Closeables.close(writer, true);
    }
  }
  
}
