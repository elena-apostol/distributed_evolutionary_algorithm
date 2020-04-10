package ro.pub.ga.watchmaker.example.clustering;

import java.text.DecimalFormat;
import java.util.ArrayList;
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
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.apache.mahout.math.WeightedVector;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.selection.RouletteWheelSelection;

import ro.pub.ga.watchmaker.example.clustering.ClusteringFileParserFactory.ClusteringFileParser;
import ro.pub.ga.watchmaker.example.clustering.ClusteringFileParserFactory.ClusteringInputType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.EvolutionType;

public class ClusteringResolver extends Configured  implements Tool{


	  private static FitnessEvaluator<List<Float>> evaluator;
	  private static Options options;
	  
	  public static void main(String[] args) throws Exception {
		  
		  int res = ToolRunner.run(new ClusteringResolver(), args);
		  System.exit(res);
	  }
	  
	  public static void checkLogger() {
		  
		  Logger logger= LogManager.getLogger(ClusteringResolver.class);
		  System.out.println(logger.getAllAppenders());
	  }
	  
	  private static CommandLine parseCLIParameters(String[] args) {
		  
		  options = new Options();
		  
		  options.addOption("ps", true, "population size[mandatory]");
		  options.addOption("el", true, "the number of top individuals to be passed from one generation" +
		  			"to the next(if not specified, all population is kept from one step to another)");
		  options.addOption("randomElitism", false, "type of elitism (BestType default)");
		  
		  options.addOption("gn", true, "number of generations[mandatory]");
		  options.addOption("i", true, "path to the input file containing the points data set[mandatory]");
		  options.addOption("p", false, "number of points[optional: if not specified all points will be considered]");
		  options.addOption("c", true, "number of centers[mandatory]");
		  options.addOption("masterslaveislands", false, "perform evolution using masterslaveislands model(default)");
		  options.addOption("micromacroislands", false, "perform evolution using distributed fitness evolution model");
		  
		  options.addOption("conf", true, "path to the hadoop configuration folder[mandatory]");
		  options.addOption("s", true, "number of slaves");
		  
		  CommandLineParser parser = new BasicParser();
		  CommandLine cmd = null;
		  try {
			
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			
			System.out.println("Bad input!");
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp( "Clustering", options);
		}
		  
		  return cmd;
	  }


	    /**
	     * Helper method for formatting a result as a string for display.
	     */
	    private static  String createResultString(String strategyDescription,
	                                      List<Float> centers,
	                                      int nrOfCenters,
	                                      double fitness,
	                                      long elapsedTime) {
	      StringBuilder buffer = new StringBuilder(100);
	      buffer.append('[');
	      buffer.append(strategyDescription);
	      buffer.append("]\n");
	      
	      buffer.append("Centers:\n");
	      int dim = centers.size() / nrOfCenters;
	      for (int i = 0; i < centers.size(); i++) {
	    	  buffer.append(String.valueOf(centers.get(i)) + ( (i+1) % dim == 0 ? "\n":" "));
	      }
	      buffer.append('\n');
	      
	      buffer.append("Distance between centers and assigned points: ");
	      buffer.append(String.valueOf(fitness));
	      buffer.append("\n");
	      
	      buffer.append("(Search Time: ");
	      double seconds = (double) elapsedTime / 1000;
	      buffer.append(String.valueOf(seconds));
	      buffer.append(" seconds)\n\n");
	      
	      
	      return buffer.toString();
	    }

		@Override
		public int run(String[] args) throws Exception {
			
			System.out.println("##### Running Clustering #####");
			
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
			  Integer populationSize, elitism, generations, numberOfPoints, slaves, numberOfCenters;
			  EvolutionType evolutionType = EvolutionType.MasterSlaveIslands;
			  HelpFormatter formatter = new HelpFormatter();
			  ElitismType elitismType = ElitismType.BestCandidates;
			  
			  if(!cmd.hasOption("ps") || !cmd.hasOption("c") || !cmd.hasOption("gn") || !cmd.hasOption("el") || !cmd.hasOption("i") || !cmd.hasOption("conf") || !cmd.hasOption("s")) {
				  
				  System.out.println("Mandatory options missing!");
				  formatter.printHelp("Clustering", options);
			  }
			  
			  try {
				  
				  populationSize = Integer.parseInt(cmd.getOptionValue("ps"));		  
			  } catch (Exception e) {
				  
				  System.out.println("Bad populations size! It should be an integer value.");
				  formatter.printHelp("Clustering", options);
				  return -1;
			  }
			  
			  try {
				  if(cmd.hasOption("el")) {
					  elitism = Integer.parseInt(cmd.getOptionValue("el"));
					  try {
						  if(cmd.hasOption("randomElitism")) {
							  elitismType = ElitismType.RandomCandidates;
						  } else {
							  elitismType = ElitismType.BestCandidates;
						  }
					  } catch (Exception e) {
						  formatter.printHelp("Clustering", options);
						  return -1;
					  }
				  }
				  else {
					  elitism = 0;
					  elitismType = ElitismType.All;
				  }
			  } catch (Exception e) {
				  System.out.println("Bad elitism value! It should be an integer value.");
				  formatter.printHelp("Clustering", options);
				  return -1;
			  }

			  try {
				  
				  generations = Integer.parseInt(cmd.getOptionValue("gn"));		  
			  } catch (Exception e) {
				  
				  System.out.println("Bad number of generations! It should be an integer value.");
				  formatter.printHelp("Clustering", options);
				  return -1;
			  }
			  
			  try {
				  
				  if(cmd.hasOption("p")) {
					  numberOfPoints = Integer.parseInt(cmd.getOptionValue("p"));
				  }
				  else {
					  numberOfPoints = Integer.MAX_VALUE;
				  }
			  } catch (Exception e) {
				  
				  System.out.println("Bad number of points! It should be an integer value.");
				  return -1;
			  }

			  try {
				  numberOfCenters = Integer.parseInt(cmd.getOptionValue("c"));		  
			  } catch (Exception e) {
				  
				  System.out.println("Bad center value! It should be an integer value.");
				  formatter.printHelp("Clustering", options);
				  return -1;
			  }
			  
			  try {
				  
				  slaves = Integer.parseInt(cmd.getOptionValue("s"));		  
			  } catch (Exception e) {
				  
				  System.out.println("Bad number of slaves! It should be an integer value.");
				  formatter.printHelp("Clustering", options);
				  return -1;
			  }
			  
			  if(cmd.hasOption("micromacroislands")) {
				  evolutionType = EvolutionType.MicroMacroSubpopulations;
			  }

			  ClusteringFileParser<Float> fileParser = ((ClusteringFileParser<Float>)
					  ClusteringFileParserFactory.getFileParser(ClusteringInputType.FarmInput));
			  List<ArrayList<Float>> points = fileParser.parseFiles(cmd.getOptionValue("i"));
			  System.out.println("Input description: " + fileParser.getDescription());
			  // preiau doar nr of points puncte 
			  points = new ArrayList<ArrayList<Float>>(points.subList(0, Math.min(numberOfPoints, points.size())));
			  System.out.println("Points used: " + fileParser.getPointsString(points));
			  
			  evaluator = new CenterListEvaluator(points, numberOfCenters);
			  
			  EvolutionaryClusteringResolver clusteringResolver = new EvolutionaryClusteringResolver(
					  evaluator, new RouletteWheelSelection(),
			          populationSize, elitism, elitismType, generations,
			          numberOfCenters, true, true, true, evolutionType, slaves);
			  
			  startTime = System.currentTimeMillis();
			  
			  List<Float> result = clusteringResolver.calculateCenters(points, new ProgressListener() {
					@Override
					public void updateProgress(double percentComplete) {
						DecimalFormat df = new DecimalFormat();
						df.setMaximumFractionDigits(2);
						df.setMinimumFractionDigits(2);
						
						System.out.println("Progress: " + df.format(percentComplete * 100));
					}
				});
			  
			  endTime = System.currentTimeMillis();
			  
			  System.out.println(createResultString(clusteringResolver.getDescription(),
					  result, numberOfCenters, evaluator.getFitness(result	, null),
					  endTime - startTime));
			
			return 0;
		}
	}