package ro.pub.ga.watchmaker.core;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionUtils;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.TerminationCondition;

import ro.pub.ga.watchmaker.jss.JobShopScheduler;
import ro.pub.ga.watchmaker.utils.FSUtils;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

public class IslandEvolutionReducer<T> extends Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text> {
	
	private List<TerminationCondition> conditions;
	private long startTime;
	private int epoch;
	private int generationCount;
	private FitnessEvaluator<T> fitnessEvaluator;
	private int eliteCount;
	private Boolean doMigration;
	private String outpath;
	private FSUtils fsUtils;
	
	@Override
	protected void setup(
			Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		conditions = StringUtils.fromString(conf.get(Constants.TERMINATION_CONDITIONS));
		startTime = conf.getLong(Constants.START_TIME, 0);
		epoch = conf.getInt(Constants.CURRENT_EPOCH, 9999999);
		generationCount = conf.getInt(Constants.GENERATION_COUNT_PROPERTY, 0);
		fitnessEvaluator = StringUtils.fromString(conf.get(Constants.FITNESS_EVALUATOR_PROPERTY));
		eliteCount = conf.getInt(Constants.ELITE_COUNT_PROPERTY, 0);
		doMigration = conf.getBoolean(Constants.MIGRATION_PROPERTY, false);
		outpath = conf.get("mapreduce.output.fileoutputformat.outputdir");
		fsUtils = new FSUtils(FileSystem.get(conf));
		
		printMemmoryStatistics();
		
		super.setup(context);
	}
	
	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<PopulationWritable<EvaluatedCandidate<T>>> arg1,
			Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		
		//Convert the input from Iterable to List to get it ready for processing;
		List<PopulationWritable<EvaluatedCandidate<T>>> populations = new ArrayList<PopulationWritable<EvaluatedCandidate<T>>>();
		Iterator<PopulationWritable<EvaluatedCandidate<T>>> iterator = arg1.iterator();
		PopulationWritable<EvaluatedCandidate<T>> crtPopulation;
		while(iterator.hasNext()) {
			
			crtPopulation = iterator.next();
			populations.add(new PopulationWritable<EvaluatedCandidate<T>>(crtPopulation.getPopulation(), crtPopulation.getData()));
		}
		
		performMigration(populations);
		
		PopulationWritable<EvaluatedCandidate<T>> evolvedPopulation = mergeSubpopulations(populations);
		
		//Sort the individuals in the evolved population according to the fitness values
		if (fitnessEvaluator.isNatural()) { // Descending values for natural fitness.
			
			Collections.sort(evolvedPopulation.getPopulation(), Collections.reverseOrder());
		} else { // Ascending values for non-natural fitness.
			
			Collections.sort(evolvedPopulation.getPopulation());
		}
		
		//Select best candidate;				
		T bestCandidate = null;
		PopulationData<T> populationData = EvolutionUtils.getPopulationData(evolvedPopulation.getPopulation(), fitnessEvaluator.isNatural(), eliteCount, epoch * generationCount, startTime);
		bestCandidate = populationData.getBestCandidate();
		
		//Write the subpopulations, after migration, and the best candidate to file
		//writeSubpopulationsToFiles should  be called first, it creates the output folder;
		writeSubpopulationsToFiles(populations);
		writeBestCandidateToFile(new EvaluatedCandidate<T>(populationData.getBestCandidate(), populationData.getBestCandidateFitness()));
		writePopulationDataToFile(populationData);
		
		//Write to file if the process should terminate or not
		boolean shouldTerminate = checkTerminationConditions(populationData);
		writeTerminationStatusToFile(shouldTerminate);
		
		if(shouldTerminate) {
			
			writeEvolvedPopulationToFile(evolvedPopulation.getPopulation());
		}
	}
	
	private void performMigration(List<PopulationWritable<EvaluatedCandidate<T>>> populations) {
		
		if(doMigration == false) {
			
			return;
		}
		
		//Perform the actual migrations; each population receives a set of new individuals in a ring manner;
		PopulationWritable<EvaluatedCandidate<T>> crtPopulation = null;
		int migratorsIndex;
		for(int i = 0; i < populations.size(); i++) {
			
			crtPopulation = populations.get(i);
			migratorsIndex = ((i + 1) < populations.size()) ? i + 1 : 0;
			crtPopulation.addToPopulation((List<EvaluatedCandidate<T>>)populations.get(migratorsIndex).getDataValue(Constants.INDIVIDUALS_TO_MIGRATE));
		}
		
	}
	
	public void writeSubpopulationsToFiles(List<PopulationWritable<EvaluatedCandidate<T>>> populations) {
		 
		 fsUtils.mkdir(outpath, false);
		 int populationIndex;
		 
		 for(PopulationWritable<EvaluatedCandidate<T>> crtPopulation: populations) {
			 
			populationIndex = (Integer)crtPopulation.getDataValue(Constants.POPULATION_INDEX_PARAMETER);
			crtPopulation.getData().clear();
			crtPopulation.putDataValue(Constants.POPULATION_INDEX_PARAMETER, populationIndex);
			fsUtils.writeToFile(
					outpath + "/subpopulation_" + populationIndex, 
					StringUtils.toString(
							new PopulationWritable<T>(
									crtPopulation.getPopulation(), 
									crtPopulation.getData(), 
									true)
					)
			);
		 }
	 }
	
	public PopulationWritable<EvaluatedCandidate<T>> mergeSubpopulations(List<PopulationWritable<EvaluatedCandidate<T>>> subpopulations) {
		
		PopulationWritable<EvaluatedCandidate<T>> population = new PopulationWritable<EvaluatedCandidate<T>>();
		
		for(int i = 0; i < subpopulations.size(); i++) {
			
			population.addToPopulation(subpopulations.get(i).getPopulation());
		}
		
		System.out.println("Merged population size: " + population.getPopulation().size());
		
		return population;
	}

	public void writeBestCandidateToFile(EvaluatedCandidate<T> bestCandidate) {
		 
		fsUtils.writeToFile(outpath + "/best_candidate", StringUtils.toString(bestCandidate));

	}
	
	public boolean checkTerminationConditions(PopulationData<T> populationData) {
		
		System.out.println("Best Candidate Fitness: " + populationData.getBestCandidateFitness());
		for(TerminationCondition t: conditions) {
			
			if(t.shouldTerminate(populationData) == true) {
				
				return true;
			}
		}
		
		return false;
	}
	
	public void writeTerminationStatusToFile(boolean shouldTerminate) {
		 
		fsUtils.writeToFile(outpath + "/should_terminate", shouldTerminate ? Constants.TRUE : Constants.FALSE);
	}
	
	public void writeEvolvedPopulationToFile(List<EvaluatedCandidate<T>> population) {
		
		fsUtils.writeToFile(outpath + "/evolved_population", StringUtils.toString(population));
	}
	
	public void writePopulationDataToFile(PopulationData<T> data) {
		
		fsUtils.writeToFile(outpath + "/population_data", StringUtils.toString(data));
	}
	
	public void printMemmoryStatistics() {
		
		//Getting the runtime reference from system
        Runtime runtime = Runtime.getRuntime();
        int mb = 1024*1024;
        Logger logger= LogManager.getLogger(this.getClass());
         
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
}
