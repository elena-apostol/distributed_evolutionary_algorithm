package ro.pub.ga.watchmaker.jss;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.mahout.common.RandomUtils;
import org.uncommons.maths.random.PoissonGenerator;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.operators.EvolutionPipeline;
import org.uncommons.watchmaker.framework.selection.RankSelection;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.core.DistributedEvolutionEngine;
import ro.pub.ga.watchmaker.core.DistributedEvolutionEngine.EvolutionType;

public class JobShopScheduler {
	
	private static Options options;
	
	private static CommandLine parseCLIParameters(String[] args) {
		  
		  options = new Options();
		  
		  options.addOption("ps", true, "population size[mandatory]");
		  options.addOption("el", true, "the number of top individuals to be passed from one generation to the next[mandatory]");
		  options.addOption("gn", true, "number of generations[mandatory]");
		  options.addOption("i", true, "path to the input file containing the cities[mandatory]");
		  options.addOption("islands", false, "perform evolution using island evolution model(default)");
		  options.addOption("distFitness", false, "perform evolution using distributed fitness evolution model");
		  options.addOption("conf", true, "path to the hadoop configuration folder[mandatory]");
		  options.addOption("s", true, "number of slaves[mandatory]");
		  
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
	
	public static void printMemmoryStatistics() {
		
		//Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        Logger logger= LogManager.getLogger(JobShopScheduler.class);
         
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
        try {
			logger.log(Priority.INFO, "[" + InetAddress.getLocalHost().getHostName() +"]Max Memory:" + runtime.maxMemory() / mb);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        //----------------------------------------------
	}

	public static void main(String[] args) {
		
		//Getting the runtime reference from system
        printMemmoryStatistics();
        
        CommandLine cmd = parseCLIParameters(args);
		  if(cmd == null) {
			  
			  return;
		  }

		  Long startTime, endTime;
		  Integer populationSize, elitism, generations, slaves;
		  EvolutionType evolutionType = EvolutionType.ISLANDS;
		  HelpFormatter formatter = new HelpFormatter();
		  
		  if(!cmd.hasOption("ps") || !cmd.hasOption("gn") || !cmd.hasOption("el") || !cmd.hasOption("i") || !cmd.hasOption("conf")) {
			  
			  System.out.println("Mandatory options missing!");
			  formatter.printHelp("TSP", options);
		  }
		  
		  try {
			  
			  populationSize = Integer.parseInt(cmd.getOptionValue("ps"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad populations size! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return;
		  }
		  
		  try {
			  
			  elitism = Integer.parseInt(cmd.getOptionValue("el"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad elitism value! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return;
		  }
		  
		  try {
			  
			  generations = Integer.parseInt(cmd.getOptionValue("gn"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad number of generations! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return;
		  }
		  
		  try {
			  
			  slaves = Integer.parseInt(cmd.getOptionValue("s"));		  
		  } catch (Exception e) {
			  
			  System.out.println("Bad number of slaves! It should be an integer value.");
			  formatter.printHelp("TSP", options);
			  return;
		  }
		  
		  if(cmd.hasOption("distFitness")) {
			  
			  evolutionType = EvolutionType.DISTRIBUTED_FITNESS;
		  }
		
		Random rng = RandomUtils.getRandom(RandomUtils.getRandom().nextLong());
		CandidateFactory<List<List<ScheduleItem>>> candidateFactory = new ScheduleFactory(cmd.getOptionValue("i"), rng);
		FitnessEvaluator<List<List<ScheduleItem>>> fitnessEvaluator = new ScheduleEvaluator(
				((ScheduleFactory)candidateFactory).getNumberOfMachines(),
				((ScheduleFactory)candidateFactory).getNumberOfJobs(), 
				rng, 
				((ScheduleFactory)candidateFactory).getJobs());
		

		List<EvolutionaryOperator<List<List<ScheduleItem>>>> operators = new ArrayList<EvolutionaryOperator<List<List<ScheduleItem>>>>(2);
		operators.add(new DoubleListCrossover<ScheduleItem>());
		operators.add(new DoubleListMutation<ScheduleItem>(new PoissonGenerator(1.7, rng), new PoissonGenerator(1.7, rng), 0.7));
		EvolutionaryOperator<List<List<ScheduleItem>>> evolutionPipeline = new EvolutionPipeline<>(operators);
		EvolutionEngine<List<List<ScheduleItem>>> engine = new DistributedEvolutionEngine<List<List<ScheduleItem>>>(
				candidateFactory, 
				evolutionPipeline , 
				fitnessEvaluator, 
				new RankSelection(), 
				rng, 
				evolutionType, 
				true, 
				//cmd.getOptionValue("conf")
				slaves);
		
		engine.addEvolutionObserver(new EvolutionObserver<List<List<ScheduleItem>>>() {

			@Override
			public void populationUpdate(
					PopulationData<? extends List<List<ScheduleItem>>> data) {
				
				System.out.println("\nGeneration: " + data.getGenerationNumber() + ", Best fitness: " + data.getBestCandidateFitness() + ", Mean fitness: " + data.getMeanFitness() + "\n");
			}

		});
		
		startTime = System.currentTimeMillis();
		List<EvaluatedCandidate<List<List<ScheduleItem>>>> evaluatedPopulation = engine.evolvePopulation(populationSize, elitism, new GenerationCount(generations));
		List<List<List<ScheduleItem>>> population = new ArrayList<List<List<ScheduleItem>>>(evaluatedPopulation.size());
		for(int i = 0; i < evaluatedPopulation.size(); i++) {
			
			population.add(evaluatedPopulation.get(i).getCandidate());
		}
		endTime = System.currentTimeMillis();
		
		ScheduleBuilder scheduleBuilder = new ScheduleBuilder(((ScheduleFactory)candidateFactory).getJobs());
		List<List<ScheduleItem>> copy = scheduleBuilder.getCopyOfCandidate(evaluatedPopulation.get(0).getCandidate());
		System.out.println("Best individual:\n" + ScheduleFactory.printSchedule(scheduleBuilder.buildSchedule(copy)));
		System.out.println("Fitness: " + fitnessEvaluator.getFitness(evaluatedPopulation.get(0).getCandidate(), population));
		System.out.println("Running time: " + (((double)(endTime - startTime)) / 1000) + " s.");
 	}
	
	public static void generateInput(CandidateFactory<List<List<ScheduleItem>>> cf) {
		
		ScheduleFactory factory = (ScheduleFactory) cf;
		List<List<Task>> jobs = factory.getJobs();

		int size = jobs.get(0).size();
		for(int i = 1; i < 7; i++) {
			
			for(int j = 0; j < jobs.size(); j ++) {
				for(int k = 0; k < size; k++) {
					
					jobs.get(j).add(new Task(jobs.get(j).get(k).getMachineNumber() + factory.getNumberOfMachines() * i, jobs.get(j).get(k).getExecutionTime()));
				}
			}
		}
		
		for(int i = 0; i < jobs.size(); i++) {
			
			StringBuilder sb = new StringBuilder("");
			for(int j = 0; j < jobs.get(i).size(); j++) {
				
				sb.append(jobs.get(i).get(j).getMachineNumber() + " " + jobs.get(i).get(j).getExecutionTime() + " ");
			}
			System.out.println(sb.toString());
		}
		
	}

}
