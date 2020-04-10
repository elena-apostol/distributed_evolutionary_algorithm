/*
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

package ro.pub.ga.watchmaker.example;

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.mahout.ga.watchmaker.travellingsalesman.DistanceLookup;
import org.apache.mahout.ga.watchmaker.travellingsalesman.EuropeanDistanceLookup;
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.apache.mahout.ga.watchmaker.travellingsalesman.RouteEvaluator;
import org.apache.mahout.ga.watchmaker.travellingsalesman.TravellingSalesmanStrategy;
import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.selection.RouletteWheelSelection;
import org.uncommons.watchmaker.framework.selection.TournamentSelection;

import ro.pub.ga.watchmaker.core.DistributedEvolutionEngine.EvolutionType;

public final class NTravellingSalesman extends Configured  implements Tool{


  private static FitnessEvaluator<List<String>> evaluator;
  private static Options options;
  

  public NTravellingSalesman() {
    DistanceLookup distances = new EuropeanDistanceLookup();
    evaluator = new RouteEvaluator(distances);
  }


  
  public static void main(String[] args) throws Exception {
	  
	  int res = ToolRunner.run(new NTravellingSalesman(), args);
	  System.exit(res);
  }
  
  public static void checkLogger() {
	  
	  Logger logger= LogManager.getLogger(NTravellingSalesman.class);
	  
	  System.out.println(logger.getAllAppenders());
  }
  
  private static CommandLine parseCLIParameters(String[] args) {
	  
	  options = new Options();
	  
	  options.addOption("ps", true, "population size[mandatory]");
	  options.addOption("el", true, "the number of top individuals to be passed from one generation to the next[mandatory]");
	  options.addOption("gn", true, "number of generations[mandatory]");
	  options.addOption("i", true, "path to the input file containing the cities[mandatory]");
	  options.addOption("c", true, "number of cities[optional: if not specified all cities will be considered]");
	  options.addOption("islands", false, "perform evolution using island evolution model(default)");
	  options.addOption("distFitness", false, "perform evolution using distributed fitness evolution model");
	  options.addOption("conf", true, "path to the hadoop configuration folder[mandatory]");
	  options.addOption("s", true, "number of slaves");
	  
	  CommandLineParser parser = new BasicParser();
	  CommandLine cmd = null;
	  try {
		
		cmd = parser.parse(options, args);
	} catch (ParseException e) {
		
		System.out.println("Bad input!");
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp( "TSP", options);
	}
	  
	  return cmd;
  }


    /**
     * Helper method for formatting a result as a string for display.
     */
    private static  String createResultString(String strategyDescription,
                                      List<String> shortestRoute,
                                      double distance,
                                      long elapsedTime) {
      StringBuilder buffer = new StringBuilder(100);
      buffer.append('[');
      buffer.append(strategyDescription);
      buffer.append("]\n");
      buffer.append("ROUTE: ");
      for (String s : shortestRoute) {
        buffer.append(s);
        buffer.append(" -> ");
      }
      buffer.append(shortestRoute.get(0));
      buffer.append('\n');
      buffer.append("TOTAL DISTANCE: ");
      buffer.append(String.valueOf(distance));
      buffer.append("km\n");
      buffer.append("(Search Time: ");
      double seconds = (double) elapsedTime / 1000;
      buffer.append(String.valueOf(seconds));
      buffer.append(" seconds)\n\n");
      return buffer.toString();
    }

	@Override
	public int run(String[] args) throws Exception {
		
		//Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
         
        System.out.println("##### Heap utilization statistics [MB] #####");
         
        //Print used memory
        System.out.println("Used Memory:"
            + (runtime.totalMemory() - runtime.freeMemory()) / mb);
 
        //Print free memory
        System.out.println("Free Memory:"
            + runtime.freeMemory() / mb);
         
        //Print total available memory
        System.out.println("Total Memory:" + runtime.totalMemory() / mb);
 
        //Print Maximum available memory
        System.out.println("Max Memory:" + runtime.maxMemory() / mb);
        //----------------------------------------------
		  
		  CommandLine cmd = parseCLIParameters(args);
		  if(cmd == null) {
			  
			  return -1;
		  }

		  Long startTime, endTime;
		  Integer populationSize, elitism, generations, numberOfCities, slaves;
		  EvolutionType evolutionType = EvolutionType.ISLANDS;
		  HelpFormatter formatter = new HelpFormatter();
		  
		  if(!cmd.hasOption("ps") || !cmd.hasOption("gn") || !cmd.hasOption("el") || !cmd.hasOption("i") || !cmd.hasOption("conf") || !cmd.hasOption("s")) {
			  
			  System.out.println("Mandatory options missing!");
			  formatter.printHelp("TSP", options);
		  }
		  
		  try {
			  
			  populationSize = Integer.parseInt(cmd.getOptionValue("ps"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad populations size! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return -1;
		  }
		  
		  try {
			  
			  elitism = Integer.parseInt(cmd.getOptionValue("el"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad elitism value! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return -1;
		  }
		  
		  try {
			  
			  generations = Integer.parseInt(cmd.getOptionValue("gn"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad number of generations! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return -1;
		  }
		  
		  try {
			  
			  if(cmd.hasOption("c")) {
				  numberOfCities = Integer.parseInt(cmd.getOptionValue("c"));
			  }
			  else {
				  numberOfCities = Integer.MAX_VALUE;
			  }
		  } catch (Exception e) {
			  
			  System.out.println("Bad number of cities! It should be an integer value.");
			  return -1;
		  }
		  
		  try {
			  
			  slaves = Integer.parseInt(cmd.getOptionValue("s"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad number of slaves! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return -1;
		  }
		  
		  if(cmd.hasOption("distFitness")) {
			  
			  evolutionType = EvolutionType.DISTRIBUTED_FITNESS;
		  }

		  DistanceLookup distanceLookup = new NDistanceLookup(cmd.getOptionValue("i"), numberOfCities);
		  evaluator = new NRouteEvaluator(distanceLookup);
		  
		  TravellingSalesmanStrategy strategy = new NEvolutionaryTravellingSalesman(distanceLookup,
				  new RouletteWheelSelection(),
				  //Oana:new org.uncommons.watchmaker.framework.selection.TournamentSelection(new Probability(0.7d)),
		          populationSize, elitism, generations, true, true, true, evolutionType, slaves);
		  
		  startTime = System.currentTimeMillis();
		  
		  List<String> result = strategy.calculateShortestRoute(distanceLookup.getKnownCities(), new ProgressListener() {
			
				@Override
				public void updateProgress(double percentComplete) {
					
					DecimalFormat df = new DecimalFormat();
					df.setMaximumFractionDigits(2);
					df.setMinimumFractionDigits(2);
					
					System.out.println("Progress: " + df.format(percentComplete * 100));
				}
			});
		  
		  endTime = System.currentTimeMillis();
		  System.out.println(createResultString(strategy.getDescription(), result, evaluator.getFitness(result, null), endTime - startTime));
		
		return 0;
	}
}
