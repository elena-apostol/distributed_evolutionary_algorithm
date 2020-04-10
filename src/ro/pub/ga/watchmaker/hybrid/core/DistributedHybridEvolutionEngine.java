package ro.pub.ga.watchmaker.hybrid.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionUtils;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;
import org.uncommons.watchmaker.framework.TerminationCondition;

import ro.pub.ga.watchmaker.core.Constants;
import ro.pub.ga.watchmaker.utils.AssignedCandidateWritable;
import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;
import ro.pub.ga.watchmaker.utils.FSUtils;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

import com.google.gson.reflect.TypeToken;

public class DistributedHybridEvolutionEngine<T> implements EvolutionEngine<T> {

	private final static int MIGRATION_COUNT_HARDCODED = 10;
	private final static int NUMBER_OF_SUBPOPULATIONS_HARDCODED = 3;
	private final static int NUMBER_OF_MICROPOPULATIONS_HARDCODED = 9;
	private final static int NUMBER_OF_MACROPOPULATIONS_HARDCODED = 3;
	private final static int NUMBER_OF_GENERATIONS_PER_EPOCH_HARDCODED = 5;
	
	private final CandidateFactory<T> candidateFactory;
	private final EvolutionaryOperator<T> evolutionScheme;
	private final FitnessEvaluator<? super T> fitnessEvaluator;
	private final SelectionStrategy<? super T> selectionStrategy;
	private final Random rng;

	private EvolutionType evolutionType = EvolutionType.MasterSlaveIslands;
	private ElitismType elitismType = ElitismType.BestCandidates;
	private ApplicationType applicationType = ApplicationType.Unknown;
	private final boolean doMigration;
	private int slaveNumber;

	private List<EvolutionObserver<? super T>> observers;

	private FSUtils fsUtils;

	private String stringifiedEvolutinScheme;
	private String stringifiedFitnessEvaluator;
	private String stringifiedSelectionStrategy;
	private String stringifiedRNG;
	private String stringifiedElitismType;
	private String stringifiedCandidateFactory;
	
	public enum EvolutionType {
    	MasterSlaveIslands, MicroMacroSubpopulations
    };
    
    // what candidates are kept from one generation to another
    public enum ElitismType {
    	BestCandidates, RandomCandidates, All
    };
    
    // used for activating application dependent optimizations
    public enum ApplicationType {
    	Clustering, Classifier, Unknown
    };
	
	public DistributedHybridEvolutionEngine(CandidateFactory<T> candidateFactory,
			EvolutionaryOperator<T> evolutionScheme,
			FitnessEvaluator<? super T> fitnessEvaluator,
			SelectionStrategy<? super T> selectionStrategy,
			Random rng,
			EvolutionType evolutionType,
			ElitismType elitismType,
			ApplicationType applicationType,
			boolean doMigration,
			String confDirectory) {

		this.candidateFactory = candidateFactory;
		this.evolutionScheme = evolutionScheme;
		this.fitnessEvaluator = fitnessEvaluator;
		this.selectionStrategy = selectionStrategy;
		this.rng = rng;
		this.evolutionType = evolutionType;
		this.elitismType = elitismType;
		this.applicationType = applicationType;
		this.doMigration = doMigration;

		try {
			this.fsUtils = new FSUtils(FileSystem.get(new Configuration()));
			this.slaveNumber = getNumberOfSlaves(confDirectory);
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.observers = new ArrayList<EvolutionObserver<? super T>>();

		this.stringifiedCandidateFactory = StringUtils.toString(candidateFactory);
		this.stringifiedEvolutinScheme = StringUtils.toString(evolutionScheme);
		this.stringifiedFitnessEvaluator = StringUtils.toString(fitnessEvaluator);
		this.stringifiedSelectionStrategy = StringUtils.toString(selectionStrategy);
		this.stringifiedRNG = StringUtils.toString(rng);
		this.stringifiedElitismType = StringUtils.toString(elitismType);
	}

	// constructor primeste nr de slaves direct
	public DistributedHybridEvolutionEngine(CandidateFactory<T> candidateFactory,
			EvolutionaryOperator<T> evolutionScheme,
			FitnessEvaluator<? super T> fitnessEvaluator,
			SelectionStrategy<? super T> selectionStrategy,
			Random rng,
			EvolutionType evolutionType,
			ElitismType elitismType,
			ApplicationType applicationType,
			boolean doMigration,
			int slaves) {

		this.candidateFactory = candidateFactory;
		this.evolutionScheme = evolutionScheme;
		this.fitnessEvaluator = fitnessEvaluator;
		this.selectionStrategy = selectionStrategy;
		this.rng = rng;
		this.evolutionType = evolutionType;
		this.doMigration = doMigration;
		this.elitismType = elitismType;
		this.applicationType = applicationType;

		try {
			this.fsUtils = new FSUtils(FileSystem.get(new Configuration()));
			this.slaveNumber = slaves;
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.observers = new ArrayList<EvolutionObserver<? super T>>();

		this.stringifiedCandidateFactory = StringUtils.toString(candidateFactory);
		this.stringifiedEvolutinScheme = StringUtils.toString(evolutionScheme);
		this.stringifiedFitnessEvaluator = StringUtils.toString(fitnessEvaluator);
		this.stringifiedSelectionStrategy = StringUtils.toString(selectionStrategy);
		this.stringifiedElitismType = StringUtils.toString(elitismType);
		this.stringifiedRNG = StringUtils.toString(rng);
	}

	@Override
	public T evolve(int populationSize, int eliteCount,
			TerminationCondition... conditions) {
		List<EvaluatedCandidate<T>> population = this.evolvePopulation(populationSize, eliteCount, new ArrayList<T>(), conditions);
		return population.get(0).getCandidate();
	}

	@Override
	public T evolve(int populationSize, int eliteCount,
			Collection<T> seedCandidates, TerminationCondition... conditions) {
		List<EvaluatedCandidate<T>> population = this.evolvePopulation(populationSize, eliteCount, seedCandidates, conditions);
		return population.get(0).getCandidate();
	}

	@Override
	public List<EvaluatedCandidate<T>> evolvePopulation(int populationSize,
			int eliteCount, TerminationCondition... conditions) {
		return this.evolvePopulation(populationSize, eliteCount, new ArrayList<T>(), conditions);
	}

	@Override
	public List<EvaluatedCandidate<T>> evolvePopulation(int populationSize,
			int eliteCount, Collection<T> seedCandidates,
			TerminationCondition... conditions) {

		switch (evolutionType) {
		case MasterSlaveIslands:
			try {
				return evolvePopulationUsingMasterSlaveIslandsModel(populationSize, eliteCount, candidateFactory.generateInitialPopulation(populationSize, seedCandidates, rng), conditions);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case MicroMacroSubpopulations:
			try {
				return evolvePopulationUsingMicroMacroIslandsModel(populationSize, eliteCount, candidateFactory.generateInitialPopulation(populationSize, seedCandidates, rng), conditions);
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		}
		return null;
	}
	
	/* ------------------------------- MasterSlaveIslandsModel--------------------------------*/
	
	public List<EvaluatedCandidate<T>> evolvePopulationUsingMasterSlaveIslandsModel(int populationSize,
			int eliteCount,
			Collection<T> seedCandidates,
			TerminationCondition... conditions) throws IOException, ClassNotFoundException, InterruptedException {

		int numberOfSubpopulations = Math.max(slaveNumber / 2, 1);
		numberOfSubpopulations = NUMBER_OF_SUBPOPULATIONS_HARDCODED;
		int migrationCount = MIGRATION_COUNT_HARDCODED;
		
		Path inputPath = new Path("input");
		Path outputPath = new Path("output");
		long startTime = System.currentTimeMillis();
		EvaluatedCandidate<T> bestCandidate = null;
		EvaluatedCandidate<T> crtBestCandidate;

		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());

		Configuration conf = job.getConfiguration();     
		HadoopUtil.delete(conf, inputPath);
		HadoopUtil.delete(conf, outputPath);

		writeSubpopulationsToFiles(splitPopulation(new ArrayList<T>(seedCandidates), numberOfSubpopulations), "input");
		
		List<EvaluatedCandidate<T>> evolvedPopulation = null;
		
		int generationCount = 0;
		do {
			generationCount++;
			Job newJob = prepareJobForMasterSlaveIslandsModel(generationCount, populationSize, eliteCount,
					migrationCount,
					startTime, numberOfSubpopulations,
					conditions);
			newJob.waitForCompletion(true);

			crtBestCandidate = getBestCandidate("output");
			bestCandidate = getBetterCandidate(crtBestCandidate, bestCandidate);
			
			evolvedPopulation = getEvolvedPopulation("output");
			updateIslandEvolutionObservers(evolvedPopulation, eliteCount, generationCount, startTime);	
		} while(!checkTerminationStatus(evolvedPopulation, eliteCount, generationCount, startTime, conditions));

		// face merge la toata subpopulatiile evaluate
		List<EvaluatedCandidate<T>> result = getEvolvedPopulation("output");
		
		result.add(0, bestCandidate);
		result.remove(result.size() - 1);
		
		if (fitnessEvaluator.isNatural()) {
			Collections.sort(result, Collections.reverseOrder());
		} else {
			Collections.sort(result);
		}
		
		System.out.println("Best candidate fitness " + result.get(0).getFitness());
		return result;
	}

	private Job prepareJobForMasterSlaveIslandsModel(int generationCount,
			int populationSize,
			int eliteCount,
			int migrationCount,
			long startTime,
			int numberOfSubpopulations,
			TerminationCondition... conditions) throws IOException {

		Path inputPath = new Path("input");
		Path outputPath = new Path("output");
		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());
		
		Configuration conf = job.getConfiguration();
		if (generationCount > 1)
			copyOutputToInput("output", "input");

		configureJobForMasterSlaveIslandsModel(job, conf, eliteCount, startTime,
				generationCount, populationSize, migrationCount, inputPath, outputPath, numberOfSubpopulations,
				conditions);

		return job;
	}

	private void configureJobForMasterSlaveIslandsModel(Job job,
			Configuration conf,
			int eliteCount,
			long startTime,
			int generationCount,
			int populationSize,
			int migrationCount,
			Path inpath,
			Path outpath,
			int numberOfSubpopulations,
			TerminationCondition... conditions) {

		conf.set("mapreduce.input.fileinputformat.inputdir", inpath.toString());
		conf.set("mapreduce.output.fileoutputformat.outputdir", outpath.toString());

		
		conf.set(Constants.EVOLUTION_SCHEME_PROPERTY, stringifiedEvolutinScheme);
		conf.set(Constants.FITNESS_EVALUATOR_PROPERTY, stringifiedFitnessEvaluator);
		conf.set(Constants.SELECTION_STRATEGY_PROPERTY, stringifiedSelectionStrategy);
		conf.set(Constants.RANDOM_GENERATOR_PROPERTY, stringifiedRNG);
		conf.setInt(Constants.GENERATION_COUNT_PROPERTY, generationCount);
		// nr de indivizi ce se pastreaza de la o generatie la alta per subpopulatie 
		conf.setInt(Constants.ELITE_COUNT_PROPERTY, ((int)Math.ceil(((double)eliteCount) / numberOfSubpopulations)));
		conf.set(Constants.ELITISM_TYPE, stringifiedElitismType);
		conf.setBoolean(Constants.MIGRATION_PROPERTY, doMigration);
		if(doMigration) {
			conf.setInt(Constants.MIGRATION_COUNT_PROPERTY, migrationCount);
		}
		conf.set(Constants.TERMINATION_CONDITIONS, StringUtils.toString(conditions));
		conf.setLong(Constants.START_TIME, startTime);
		
		conf.setInt(Constants.NUM_SLAVES, slaveNumber);
		conf.setInt(Constants.NUM_SUBPOPULATIONS, numberOfSubpopulations);
		conf.set(Constants.APPLICATION_TYPE, StringUtils.toString(applicationType));

		TypeToken<EvaluatedCandidateWritable<T>> evalCandidateTypeToken = new TypeToken<EvaluatedCandidateWritable<T>>() {};
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(evalCandidateTypeToken.getRawType());
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);

		TypeToken<MasterSlaveIslandsMapper<T>> mapperTypeToken = new TypeToken<MasterSlaveIslandsMapper<T>>() {};
		TypeToken<MasterSlaveIslandsReducer<T>> reducerTypeToken = new TypeToken<MasterSlaveIslandsReducer<T>>() {};
		
		job.setMapperClass((Class<? extends Mapper>) mapperTypeToken.getRawType());
		job.setReducerClass((Class<? extends Reducer>) reducerTypeToken.getRawType());
		job.setNumReduceTasks(numberOfSubpopulations); 
		
		job.setInputFormatClass(TextInputFormat.class);
	}

	
	
	/* ------------------------------- MicroMacroIslandsModel--------------------------------*/
	
	public List<EvaluatedCandidate<T>> evolvePopulationUsingMicroMacroIslandsModel(int populationSize,
			int eliteCount,
			Collection<T> seedCandidates,
			TerminationCondition... conditions) throws IOException, ClassNotFoundException, InterruptedException {

		int numberOfMicroPopulations = slaveNumber;
		
		numberOfMicroPopulations = NUMBER_OF_MICROPOPULATIONS_HARDCODED;
		int numberOfMacroPopulations = NUMBER_OF_MACROPOPULATIONS_HARDCODED;//??
		// Half of the migrators of a micropopulation will migrate to another micropopulation
		// inside the microislands and the other half will migrate to another mcropopulation
		int migrationCount = MIGRATION_COUNT_HARDCODED;
		int nrGenerationsPerEpoch = NUMBER_OF_GENERATIONS_PER_EPOCH_HARDCODED;
		
		Path inputPath = new Path("input");
		Path outputPath = new Path("output");
		
		long startTime = System.currentTimeMillis();
		
		EvaluatedCandidate<T> bestCandidate = null;
		EvaluatedCandidate<T> crtBestCandidate;

		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());

		Configuration conf = job.getConfiguration();     
		HadoopUtil.delete(conf, inputPath);
		HadoopUtil.delete(conf, outputPath);
	
		writeMicroPopulationsToFiles(splitPopulationIntoMicroPopulations(new ArrayList<T>(seedCandidates),
				numberOfMicroPopulations, numberOfMacroPopulations), "input");
		
		List<EvaluatedCandidate<T>> evolvedPopulation = null;
		
		int epochCount = 0;
		do {
			epochCount++;
			Job newJob = prepareJobForMicroMacroIslandsModel(nrGenerationsPerEpoch,
					epochCount, eliteCount,
					migrationCount,
					startTime, numberOfMicroPopulations,
					numberOfMacroPopulations,
					conditions);
			newJob.waitForCompletion(true);

			crtBestCandidate = getBestCandidate("output");
			bestCandidate = getBetterCandidate(crtBestCandidate, bestCandidate);
			
			evolvedPopulation = getEvolvedPopulation("output");
			
			updateIslandEvolutionObservers(evolvedPopulation, eliteCount, nrGenerationsPerEpoch * epochCount, startTime);	
		} while(!checkTerminationStatus(evolvedPopulation, eliteCount, nrGenerationsPerEpoch * epochCount, startTime, conditions));

		// face merge la toata subpopulatiile evaluate
		List<EvaluatedCandidate<T>> result = getEvolvedPopulation("output");
		
		result.add(0, bestCandidate);
		result.remove(result.size() - 1);
		System.out.println("Best candidate fitness " + result.get(0).getFitness());
		return result;
	}
	
	private Job prepareJobForMicroMacroIslandsModel(int generationCount,
			int epochCount,
			int eliteCount,
			int migrationCount,
			long startTime,
			int numberOfMicroIslands,
			int numberOfMacroIslands,
			TerminationCondition... conditions) throws IOException {

		Path inputPath = new Path("input");
		Path outputPath = new Path("output");
		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());
		
		Configuration conf = job.getConfiguration();
		if (epochCount > 1)
			copyOutputToInput("output", "input");

		configureJobForMicroMacroIslandsModel(job, conf, eliteCount, startTime,
				generationCount, epochCount, migrationCount, inputPath, outputPath, numberOfMicroIslands,
				numberOfMacroIslands, conditions);

		return job;
	}
	
	private void configureJobForMicroMacroIslandsModel(Job job,
			Configuration conf,
			int eliteCount,
			long startTime,
			int generationCount,
			int epochCount,
			int migrationCount,
			Path inpath,
			Path outpath,
			int numberOfMicroIslands,
			int numberOfMacroIslands,
			TerminationCondition... conditions) {

		conf.set("mapreduce.input.fileinputformat.inputdir", inpath.toString());
		conf.set("mapreduce.output.fileoutputformat.outputdir", outpath.toString());
		
		conf.set(Constants.CANDIDATE_FACTORY_PROPERTY, stringifiedCandidateFactory);
		conf.set(Constants.EVOLUTION_SCHEME_PROPERTY, stringifiedEvolutinScheme);
		conf.set(Constants.FITNESS_EVALUATOR_PROPERTY, stringifiedFitnessEvaluator);
		conf.set(Constants.SELECTION_STRATEGY_PROPERTY, stringifiedSelectionStrategy);
		conf.set(Constants.RANDOM_GENERATOR_PROPERTY, stringifiedRNG);
		conf.setInt(Constants.GENERATION_COUNT_PROPERTY, generationCount);
		conf.setInt(Constants.ELITE_COUNT_PROPERTY, ((int)Math.ceil(((double)eliteCount) / numberOfMicroIslands)));
		conf.set(Constants.ELITISM_TYPE, stringifiedElitismType);
		conf.setBoolean(Constants.MIGRATION_PROPERTY, doMigration);
		if(doMigration) {
			conf.setInt(Constants.MIGRATION_COUNT_PROPERTY, migrationCount);
		}
		conf.set(Constants.TERMINATION_CONDITIONS, StringUtils.toString(Arrays.asList(conditions)));

		conf.setLong(Constants.START_TIME, startTime);
		
		conf.setInt(Constants.NUM_SLAVES, slaveNumber);
		conf.setInt(Constants.NUM_MICROISLANDS, numberOfMicroIslands);
		conf.setInt(Constants.NUM_MACROISLANDS, numberOfMacroIslands);
		conf.setInt(Constants.CURRENT_EPOCH, epochCount);
		conf.set(Constants.APPLICATION_TYPE, StringUtils.toString(applicationType));
		
		TypeToken<PopulationWritable<T>> populationTypeToken = new TypeToken<PopulationWritable<T>>() {};
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(populationTypeToken.getRawType());
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		TypeToken<MicroMacroIslandsMapper<T>> mapperTypeToken = new TypeToken<MicroMacroIslandsMapper<T>>() {};
		TypeToken<MicroMacroIslandsReducer<T>> reducerTypeToken = new TypeToken<MicroMacroIslandsReducer<T>>() {};
		
		job.setMapperClass((Class<? extends Mapper>) mapperTypeToken.getRawType());
		job.setReducerClass((Class<? extends Reducer>) reducerTypeToken.getRawType());
		job.setNumReduceTasks(numberOfMacroIslands); 
		
		job.setInputFormatClass(TextInputFormat.class);
	}
	
	/* --------------------------------------- Utils ----------------------------------------*/
	
	private boolean checkTerminationStatus(List<EvaluatedCandidate<T>> evolvedPopulation,
			int eliteCount, int generationCount, long startTime, TerminationCondition... conditions) {

		PopulationData<T> populationData = EvolutionUtils.getPopulationData(evolvedPopulation,
				fitnessEvaluator.isNatural(), eliteCount, generationCount, startTime);
		for (TerminationCondition condition : conditions) {
			if (condition.shouldTerminate(populationData)) {
				System.out.println("[Oana] termination condition satisfied");
				return true;
			}
		}
		return false;
	}

	public List<AssignedCandidateWritable<T>> splitPopulation(List<T> population, int numberOfSplits) {

		//List<ArrayList<T>> subpopulations = new ArrayList<ArrayList<T>>(numberOfSplits);
		int splitSize = population.size() / numberOfSplits;

		List<AssignedCandidateWritable<T>> assignedCandidates = new ArrayList<AssignedCandidateWritable<T>>();
		
		//Create the first numberOfSplits - 1 subpopulations;
		for(int i = 0; i < numberOfSplits; i++) {
			//subpopulations.add(new ArrayList<T>());
			for(int j = 0; j < splitSize; j++) {
				//subpopulations.get(i).add(population.get(i * splitSize + j));
				AssignedCandidateWritable<T> assignedCandidate =
					new AssignedCandidateWritable<T>(population.get(i * splitSize + j), new Long(i));
				assignedCandidates.add(assignedCandidate);
			}
		}

		//Create the last subpopulation;
		//This is created separately because the populationSize might not divide exactly to the numberOfSplits;
		int processedCandidates = numberOfSplits * splitSize;
		for(int i = processedCandidates; i < population.size(); i++) {
			//subpopulations.get(i - processedCandidates).add(population.get(i));
			AssignedCandidateWritable<T> assignedCandidate =
				new AssignedCandidateWritable<T>(population.get(i), new Long(i - processedCandidates));
			assignedCandidates.add(assignedCandidate);
		}
		return assignedCandidates;
	}

	public List<PopulationWritable<T>> splitPopulationIntoMicroPopulations(List<T> population,
			int numberOfMicropopulations,
			int numberOfMacropopulations) {
		 
		 List<PopulationWritable<T>> micropopulations = new ArrayList<PopulationWritable<T>>(numberOfMicropopulations);
		 
		 int splitSize = population.size() / numberOfMicropopulations;
		 
		 //Create the first numberOfSplits - 1 subpopulations;
		 for(int i = 0; i < numberOfMicropopulations; i++) {
			 micropopulations.add(new PopulationWritable<T>());
			 micropopulations.get(i).putDataValue(Constants.MACROPOPULATION_INDEX, (int) i % numberOfMacropopulations);
			 micropopulations.get(i).putDataValue(Constants.MICROPOPULATION_INDEX, (int)(i / numberOfMacropopulations));
			 for(int j = 0; j < splitSize; j++) {
				 micropopulations.get(i).getPopulation().add(population.get(i * splitSize + j));
			 }
		 }
		 
		 //Create the last subpopulation;
		 //This is created separately because the populationSize might not divide exactly to the numberOfSplits;
		 int processedCandidates = numberOfMicropopulations * splitSize;
		 for(int i = processedCandidates; i < population.size(); i++) {
			 micropopulations.get(i - processedCandidates).getPopulation().add(population.get(i));
		 }
		 return micropopulations;
	 }
	 
	
	// scrie indivizii cate unul per linie in fisier coresp subpop lui
	public void writeSubpopulationsToFiles(List<AssignedCandidateWritable<T>> assignedCandidates, String inputFolder) {

		String subFolder = inputFolder;
		File subDirectory = new File(subFolder);
		if(!subDirectory.exists()) {
			subDirectory.mkdir();
		} 
		else {
			try {
				FileUtils.deleteDirectory(subDirectory);
				FileUtils.forceMkdir(subDirectory);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// vreau nr_slaves fisiere - in fiecare scriu nr de indivizi / nr_slaves
		int slice_dim = (int)Math.ceil(assignedCandidates.size() / slaveNumber);
		for (int i = 0; i < slaveNumber; i++) {
			StringBuilder sb = new StringBuilder();
			for(int j = i * slice_dim; j < Math.min((i+1) * slice_dim, assignedCandidates.size()); j++) {
				sb.append(StringUtils.toString(assignedCandidates.get(j)) + "\n");
			}
			fsUtils.writeToFile(subDirectory.getPath() + "/input_" + i, sb.toString());
		}
	}
	
	public void writeMicroPopulationsToFiles(List<PopulationWritable<T>> micropopulations, String inputFolder) {
		 
		 String subFolder = inputFolder;
		 File subDirectory = new File(subFolder);
		 if(!subDirectory.exists()) {
			 subDirectory.mkdir();
		 } 
		 else {
			 try {
				FileUtils.deleteDirectory(subDirectory);
				FileUtils.forceMkdir(subDirectory);
			} catch (IOException e) {
				e.printStackTrace();
			}
		 }
		 
		 for(int i = 0; i < micropopulations.size(); i++) {
			fsUtils.writeToFile(subDirectory.getPath() + "/subpopulation_" + i, StringUtils.toString(micropopulations.get(i)));
		 }
	 }

	//returneaza cel mai bun candidat din cei mai buni din subpopulatii
	private EvaluatedCandidate<T> getBestCandidate(String outpath) {
		EvaluatedCandidate<T> bestCandidate = null;
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(outpath));
			for(int i = 0; i < status.length; i++) {
				if(status[i].getPath().getName().startsWith("best_candidate_")) {
					EvaluatedCandidate<T> crtCandidate = StringUtils.fromString(fsUtils.readFromFile(status[i].getPath().toString()));
					bestCandidate = getBetterCandidate(bestCandidate, crtCandidate);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return bestCandidate;
	}

	// ar trebui mutat in Evaluated Candidate dar nu avem si fitnessEvaluator acolo
	// poate un comparator - care primeste in constructor fitnessevaluator
	private EvaluatedCandidate<T> getBetterCandidate(EvaluatedCandidate<T> candidate1, EvaluatedCandidate<T> candidate2) {
		if (candidate1 == null)
			return candidate2;
		if (candidate2 == null)
			return candidate1;
		if (fitnessEvaluator.isNatural()) {
			if (candidate1.getFitness() > candidate2.getFitness()) {
				return candidate1;
			} else {
				return candidate2;
			}
		} else {
			if (candidate1.getFitness() < candidate2.getFitness()) {
				return candidate1;
			} else {
				return candidate2;
			}
		}
	}

	//face merge intre subpopulatiile evaluate
	private List<EvaluatedCandidate<T>> getEvolvedPopulation(String outpath){
		List<EvaluatedCandidate<T>> population = new ArrayList<EvaluatedCandidate<T>>();
		try {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(outpath));
			for(int i = 0; i < status.length; i++) {
				if(status[i].getPath().getName().startsWith("evolved_population_")) {
					List<EvaluatedCandidate<T>> evolvedSubpopulation = StringUtils.fromString(fsUtils.readFromFile(status[i].getPath().toString()));
					population.addAll(evolvedSubpopulation);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		return population;
	}

	private void copyOutputToInput(String outpath, String inpath) {

		try {
			FileSystem fs = FileSystem.get(new Configuration());

			fs.delete(new Path(inpath), true);
			fs.mkdirs(new Path(inpath));

			FileStatus[] status = fs.listStatus(new Path(outpath));
			String data = null;
			for(int i = 0; i < status.length; i++) {
				if(status[i].getPath().getName().startsWith("subpopulation_")) {
					data = fsUtils.readFromFile(status[i].getPath().toString());
					fsUtils.writeToFile(inpath + "/" + status[i].getPath().getName(), data);
				}
			}
			fs.delete(new Path(outpath), true);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	// face merge la subpop si update la oberveri
	private void updateIslandEvolutionObservers(List<EvaluatedCandidate<T>> evaluatedPopulation,
			int eliteCount, int generationCount, long startTime) {
		//TODO we don;t need
		Collections.sort(evaluatedPopulation);
		PopulationData<T> populationData = EvolutionUtils.getPopulationData(evaluatedPopulation,
				fitnessEvaluator.isNatural(), eliteCount, generationCount, startTime);
		for(EvolutionObserver<? super T> observer: observers) {
			observer.populationUpdate(populationData);
		}
	}

	@Override
	public void addEvolutionObserver(EvolutionObserver<? super T> observer) {
		this.observers.add(observer);
	}

	@Override
	public void removeEvolutionObserver(EvolutionObserver<? super T> observer) {
		this.observers.remove(observer);
	}

	@Override
	public List<TerminationCondition> getSatisfiedTerminationConditions() {
		return null;
	}

	public static int getNumberOfSlaves(String confDirectory) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(new File(confDirectory + "/slaves")));
		int slaves = 0;
		while(reader.readLine() != null) {
			slaves++;
		}
		reader.close();
		return slaves - 1; // Yarn uses one of the slaves as MRAppMaster
	}

}
