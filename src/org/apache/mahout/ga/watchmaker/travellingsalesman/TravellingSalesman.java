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

package org.apache.mahout.ga.watchmaker.travellingsalesman;

import java.text.DecimalFormat;
import java.util.List;

import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.selection.TournamentSelection;

/**
 * Applet for comparing evolutionary and brute force approaches to the Travelling Salesman problem.
 * 
 * The original code is from <b>the Watchmaker project</b> (https://watchmaker.dev.java.net/). <br>
 * This class has been modified to add a main function that runs the JApplet inside a JDialog.
 */
public final class TravellingSalesman  {


  private static FitnessEvaluator<List<String>> evaluator;
  

  public TravellingSalesman() {
    DistanceLookup distances = new EuropeanDistanceLookup();
    evaluator = new RouteEvaluator(distances);
  }


  
  public static void main(String[] args) {
	  
	  if(args.length == 0) {
		  
		  System.out.println("Usage: java -jar <jarName> <populationSize> <elitism> <numberOfGenerations>");
		  return;
	  }

	  
	  Long startTime, endTime;
	  Integer populationSize, elitism, generations;
	  evaluator = new RouteEvaluator(new EuropeanDistanceLookup());
	  startTime = System.currentTimeMillis();
	  
	  try {
		  
		  populationSize = Integer.parseInt(args[0]);		  
	  } catch (Exception e) {
		  
		  System.out.println("Bad populations size! It should be an integer value.");
		  return;
	  }
	  
	  try {
		  
		  elitism = Integer.parseInt(args[1]);		  
	  } catch (Exception e) {
		  
		  System.out.println("Bad elitism value! It should be an integer value.");
		  return;
	  }
	  
	  try {
		  
		  generations = Integer.parseInt(args[2]);		  
	  } catch (Exception e) {
		  
		  System.out.println("Bad number of generations! It should be an integer value.");
		  return;
	  }
	  
	  TravellingSalesmanStrategy strategy = new EvolutionaryTravellingSalesman(new EuropeanDistanceLookup(),
			  new TournamentSelection(new Probability(0.95)),
	          populationSize, elitism, generations, true, true, true);
	  
	  List<String> result = strategy.calculateShortestRoute(new EuropeanDistanceLookup().getKnownCities(), new ProgressListener() {
		
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
}
