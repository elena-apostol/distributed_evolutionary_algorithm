package ro.pub.ga.watchmaker.core;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Field;
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
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
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
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;
import ro.pub.ga.watchmaker.utils.FSUtils;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

import com.google.gson.reflect.TypeToken;

public class DistributedEvolutionEngine<T> implements EvolutionEngine<T>{
	
	private final EvolutionaryOperator<T> evolutionScheme;
    private final FitnessEvaluator<? super T> fitnessEvaluator;
    private final SelectionStrategy<? super T> selectionStrategy;
    private final CandidateFactory<T> candidateFactory;
    private List<EvolutionObserver<? super T>> observers;
    private final Random rng;
    private final boolean doMigration;
    private FSUtils fsUtils;
    private int slaveNumber;
    
    public enum EvolutionType {
    	
    	DISTRIBUTED_FITNESS, ISLANDS
    };
    
    private EvolutionType evolutionType = EvolutionType.ISLANDS;
    
    private String stringifiedEvolutinScheme;
    private String stringifiedFitnessEvaluator;
    private String stringifiedSelectionStrategy;
    private String stringifiedCandidateFactory;
    private String stringifiedTerminationConditions;
    private String stringifiedRNG;

    public DistributedEvolutionEngine(CandidateFactory<T> candidateFactory,
                                       EvolutionaryOperator<T> evolutionScheme,
                                       FitnessEvaluator<? super T> fitnessEvaluator,
                                       SelectionStrategy<? super T> selectionStrategy,
                                       Random rng,
                                       EvolutionType evolutionType,
                                       boolean doMigration,
                                       String confDirectory)
    {
 
        this.evolutionScheme = evolutionScheme;
        this.fitnessEvaluator = fitnessEvaluator;
        this.selectionStrategy = selectionStrategy;
        this.evolutionType = evolutionType;
        this.candidateFactory = candidateFactory;
        this.rng = rng;
        this.doMigration = doMigration;
        try {
        	
			this.fsUtils = new FSUtils(FileSystem.get(new Configuration()));
			this.slaveNumber = getNumberOfSlaves(confDirectory);
		} catch (IOException e) {
			
			e.printStackTrace();
		}
        
        this.observers = new ArrayList<EvolutionObserver<? super T>>();
        
        this.stringifiedEvolutinScheme = StringUtils.toString(evolutionScheme);
        this.stringifiedFitnessEvaluator = StringUtils.toString(fitnessEvaluator);
        this.stringifiedSelectionStrategy = StringUtils.toString(selectionStrategy);
        this.stringifiedCandidateFactory = StringUtils.toString(candidateFactory);
        this.stringifiedRNG = StringUtils.toString(rng);
    }
    
    
    public DistributedEvolutionEngine(CandidateFactory<T> candidateFactory,
            EvolutionaryOperator<T> evolutionScheme,
            FitnessEvaluator<? super T> fitnessEvaluator,
            SelectionStrategy<? super T> selectionStrategy,
            Random rng,
            EvolutionType evolutionType,
            boolean doMigration,
            int slaves){
		
		this.evolutionScheme = evolutionScheme;
		this.fitnessEvaluator = fitnessEvaluator;
		this.selectionStrategy = selectionStrategy;
		this.evolutionType = evolutionType;
		this.candidateFactory = candidateFactory;
		this.rng = rng;
		this.doMigration = doMigration;
		try {
		
		this.fsUtils = new FSUtils(FileSystem.get(new Configuration()));
		this.slaveNumber = slaves;
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		
		this.observers = new ArrayList<EvolutionObserver<? super T>>();
		
		this.stringifiedEvolutinScheme = StringUtils.toString(evolutionScheme);
		this.stringifiedFitnessEvaluator = StringUtils.toString(fitnessEvaluator);
		this.stringifiedSelectionStrategy = StringUtils.toString(selectionStrategy);
		this.stringifiedCandidateFactory = StringUtils.toString(candidateFactory);
		this.stringifiedRNG = StringUtils.toString(rng);
		
		System.out.println(evolutionType);
	}
    
    @Override
    public List<EvaluatedCandidate<T>> evolvePopulation(int populationSize,
            int eliteCount,
            Collection<T> seedCandidates,
            TerminationCondition... conditions) {
    	
    	stringifiedTerminationConditions = StringUtils.toString(Arrays.asList(conditions));
    	
    	switch (evolutionType) {
		case DISTRIBUTED_FITNESS:
			
			try {
				return evolvePopulationWithDistributedFitness(populationSize, eliteCount, candidateFactory.generateInitialPopulation(populationSize, seedCandidates, rng), conditions);
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			break;
		case ISLANDS:
			
			try {
				return evolvePopulationWithIslandEvolution(populationSize, eliteCount, candidateFactory.generateInitialPopulation(populationSize, seedCandidates, rng), conditions);
			} catch (ClassNotFoundException e) {
				
				e.printStackTrace();
			} catch (IOException e) {
				
				e.printStackTrace();
			} catch (InterruptedException e) {

				e.printStackTrace();
			}
		}
    	
    	
    	return null;
    	
    }
    
    public List<EvaluatedCandidate<T>> evolvePopulationWithDistributedFitness(int populationSize,
            int eliteCount,
            Collection<T> seedCandidates,
            TerminationCondition... conditions) throws IOException, ClassNotFoundException, InterruptedException {
    
    	Path input = new Path("input");
    	Path output = new Path("output");
    	boolean terminate = false;
    	long startTime = System.currentTimeMillis();
    	int generation = 0;
		List<T> population = new ArrayList<T>(seedCandidates);
		List<EvaluatedCandidate<T>> evolvedPopulation;
		
		double bestFitness = fitnessEvaluator.isNatural() ? Double.MIN_VALUE : Double.MAX_VALUE;
		T bestCandidate = null;
		
		
		do {
			
			//Get the configured job for the current generation to run
	    	Job job = getJobForDistributedFitness(populationSize, input, output);		
			fsUtils.mkdir("input");
			fsUtils.delete("output");
			
			//Write the individuals to the input file for the mappers, each one on a separate line
	    	writeIndividualsToFile(population, "input/population");
	    	
	    	job.waitForCompletion(true);
	    	
	    	//Receive the population of evaluated candidates, also containing the offsprings
	    	evolvedPopulation = readAndMergePopulationPartitionsFromFiles("output");
	    	
	    	//Sort the population so the best individual is on position 0
	    	if(fitnessEvaluator.isNatural()) {
	    		
	    		Collections.sort(evolvedPopulation, Collections.reverseOrder());
	    	}
	    	else {
	    		
	    		Collections.sort(evolvedPopulation);
	    	}
	    	
	    	PopulationData<T> populationData = EvolutionUtils.getPopulationData(
	    			evolvedPopulation.subList(0, populationSize), 
	    			fitnessEvaluator.isNatural(), 
	    			eliteCount, 
	    			generation, 
	    			startTime);
	    	

	    	//Preserve the best candidate
	    	if(fitnessEvaluator.isNatural()) {
	    		
	    		if(populationData.getBestCandidateFitness() > bestFitness) {
	    			
	    			bestFitness = populationData.getBestCandidateFitness();
	    			bestCandidate = populationData.getBestCandidate();
	    		}
	    	}
	    	else {
	    		
	    		if(populationData.getBestCandidateFitness() < bestFitness) {
	    			
	    			bestFitness = populationData.getBestCandidateFitness();
	    			bestCandidate = populationData.getBestCandidate();
	    		}
	    	}
	    	
	    	for(TerminationCondition terminationCondition: conditions) {
	    		
	    		if(terminationCondition.shouldTerminate(populationData)) {
	    			
	    			terminate = true;
	    			break;
	    		}
	    	}
	    	
	    	//Set the population for the next cycle, if the termination conditions are not met;
	    	population.clear();
	    	for(int i = 0; i < populationSize; i++) {
	    		
	    		population.add(evolvedPopulation.get(i).getCandidate());
	    	}
	    	
	    	updateDistributedFitnessObservers(populationData);
	    	
	    	generation++;
		} while(!terminate);
    	
		//Prepare the returned population
		List<EvaluatedCandidate<T>> result = evolvedPopulation.subList(0, populationSize - 1);
		
		//Add the best candidate on the first position
		result.add(0, new EvaluatedCandidate<T>(bestCandidate, bestFitness));
		
    	return result;
    }
    
    public Job getJobForDistributedFitness(int populationSize, Path input, Path output) throws IOException {
    	
		Job job = Job.getInstance();
		job.setJarByClass(this.getClass());
		
		Configuration conf = job.getConfiguration();		
		
		configureJobForDistributedFitness(job, conf, populationSize,input, output);
    	
    	return job;
    }
    

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void configureJobForDistributedFitness(Job job,
						            Configuration conf,
						            int populationSize,
						            Path inpath,
						            Path outpath) {
    	
    	conf.set("mapreduce.input.fileinputformat.inputdir", inpath.toString());
        conf.set("mapreduce.output.fileoutputformat.outputdir", outpath.toString());
        conf.set(Constants.RANDOM_GENERATOR_PROPERTY, stringifiedRNG);
        conf.set(Constants.SELECTION_STRATEGY_PROPERTY, stringifiedSelectionStrategy);
        conf.set(Constants.FITNESS_EVALUATOR_PROPERTY, stringifiedFitnessEvaluator);
        conf.set(Constants.EVOLUTION_SCHEME_PROPERTY, stringifiedEvolutinScheme);
        
        //Set he N value for the NLineInputFormat
        conf.setInt("mapreduce.input.lineinputformat.linespermap", populationSize /slaveNumber); 
        
        TypeToken<EvaluatedCandidateWritable<T>> evalCandidateTypeToken = new TypeToken<EvaluatedCandidateWritable<T>>() {};
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(evalCandidateTypeToken.getRawType());
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        
        TypeToken<DistributedFitnessMapper<T>> mapperTypeToken = new TypeToken<DistributedFitnessMapper<T>>() {};
        TypeToken<DistributedFitnessPartitioner<T>> partitionerTypeToken = new TypeToken<DistributedFitnessPartitioner<T>>() {};
        TypeToken<DistributedFitnessReducer<T>> reducerTypeToken = new TypeToken<DistributedFitnessReducer<T>>() {};
        
        job.setMapperClass((Class<? extends Mapper>) mapperTypeToken.getRawType());
        job.setPartitionerClass((Class<? extends Partitioner>)partitionerTypeToken.getRawType());
        job.setReducerClass((Class<? extends Reducer>)reducerTypeToken.getRawType());
        job.setNumReduceTasks(slaveNumber); 
        
        job.setInputFormatClass(NLineInputFormat.class);
    }
	 
	 
	 
	 
	 
    

    public List<EvaluatedCandidate<T>> evolvePopulationWithIslandEvolution(int populationSize,
            int eliteCount,
            Collection<T> seedCandidates,
            TerminationCondition... conditions) throws ClassNotFoundException, IOException, InterruptedException {
    	
    	
    	Path inputPath = new Path("input");
    	Path outputPath = new Path("output");
    	long startTime = System.currentTimeMillis();
    	int generationCount = getGenerationCount(conditions);
    	EvaluatedCandidate<T> bestCandidate = null;
    	EvaluatedCandidate<T> crtBestCandidate;
    	
    	if(generationCount > 0) {
    		
    		for(int i = 5; i >= 1; i--) {
    			
    			if(generationCount / i >= 20) {
    				
    				generationCount /= i;
    				break;
    			}
    		}
    	}
    	else {
    		
    		generationCount = 20;
    	}
    	
    	Job job = Job.getInstance();
        job.setJarByClass(this.getClass());

        Configuration conf = job.getConfiguration();     
        HadoopUtil.delete(conf, inputPath);
        HadoopUtil.delete(conf, outputPath);

        PopulationWritable<T> population = new PopulationWritable<T>(new ArrayList<T>(seedCandidates));
        writeSubpopulationsToFiles(splitPopulation(population, slaveNumber), "input");
        
        //Epoch 1
        configureJobForIslandEvolution(job, conf, eliteCount,startTime, 1, generationCount, populationSize / (5 * slaveNumber),inputPath, outputPath);
        
        job.waitForCompletion(true);
        bestCandidate = StringUtils.fromString(fsUtils.readFromFile("output/best_candidate"));
        updateIslandEvolutionObservers();
        
        //Next iterations
        int epoch = 2;
        while(!checkTerminationStatus()) {
        	
        	System.out.println("Epoch: " + epoch);
        	Job newJob = prepareJob(epoch++, generationCount, eliteCount, populationSize / (5 * slaveNumber),startTime, conditions);
        	newJob.waitForCompletion(true);
        	
        	crtBestCandidate = StringUtils.fromString(fsUtils.readFromFile("output/best_candidate"));
        	if(fitnessEvaluator.isNatural()) {
        		
        		if(crtBestCandidate.getFitness() > bestCandidate.getFitness()) {
        			
        			bestCandidate = crtBestCandidate;
        		}
        	}
        	else {
        		
        		if(crtBestCandidate.getFitness() < bestCandidate.getFitness()) {
        			
        			bestCandidate = crtBestCandidate;
        		}
        	}
        	
        	updateIslandEvolutionObservers();
        }
        
        List<EvaluatedCandidate<T>> result = StringUtils.fromString(fsUtils.readFromFile("output/evolved_population"));
        result.add(0, bestCandidate);
        result.remove(populationSize);
        
    	return result;
    }
    
    private Job prepareJob(int epoch,
    		int generationCount,
    		int eliteCount,
    		int migrationCount,
    		long startTime,
    		TerminationCondition... conditions) throws IOException {
    	
    	Path inputPath = new Path("input");
    	Path outputPath = new Path("output");
    	Job job = Job.getInstance();
        job.setJarByClass(this.getClass());

        Configuration conf = job.getConfiguration();

        copyOutputToInput("output", "input");
        
        configureJobForIslandEvolution(job, conf, eliteCount, startTime, epoch, generationCount, migrationCount, inputPath, outputPath);
        
        return job;
    }
    
    @SuppressWarnings({"unchecked", "rawtypes"})
	private void configureJobForIslandEvolution(Job job,
            Configuration conf,
            int eliteCount,
            long startTime,
            int epoch,
            int generationCount,
            int migrationCount,
            Path inpath,
            Path outpath) {
		
		conf.set("mapreduce.input.fileinputformat.inputdir", inpath.toString());
        conf.set("mapreduce.output.fileoutputformat.outputdir", outpath.toString());
		
		conf.set(Constants.CANDIDATE_FACTORY_PROPERTY, stringifiedCandidateFactory);
		conf.set(Constants.EVOLUTION_SCHEME_PROPERTY, stringifiedEvolutinScheme);
		conf.set(Constants.FITNESS_EVALUATOR_PROPERTY, stringifiedFitnessEvaluator);
		conf.set(Constants.SELECTION_STRATEGY_PROPERTY, stringifiedSelectionStrategy);
		conf.set(Constants.RANDOM_GENERATOR_PROPERTY, stringifiedRNG);
		conf.setInt(Constants.GENERATION_COUNT_PROPERTY, generationCount);
		conf.setInt(Constants.ELITE_COUNT_PROPERTY, ((int)Math.ceil(((double)eliteCount) / slaveNumber)));
		conf.setBoolean(Constants.MIGRATION_PROPERTY, doMigration);
		if(doMigration) {
			
			conf.setInt(Constants.MIGRATION_COUNT_PROPERTY, migrationCount);
		}
		conf.set(Constants.TERMINATION_CONDITIONS, stringifiedTerminationConditions);
		conf.setLong(Constants.START_TIME, startTime);
		conf.setInt(Constants.CURRENT_EPOCH, epoch);
		
		TypeToken<PopulationWritable<T>> populationTypeToken = new TypeToken<PopulationWritable<T>>() {};
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(populationTypeToken.getRawType());
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		TypeToken<IslandEvolutionMapper<T>> mapperTypeToken = new TypeToken<IslandEvolutionMapper<T>>() {};
		TypeToken<IslandEvolutionReducer<T>> reducerTypeToken = new TypeToken<IslandEvolutionReducer<T>>() {};
		job.setMapperClass((Class<? extends Mapper>) mapperTypeToken.getRawType());
		job.setReducerClass((Class<? extends Reducer>) reducerTypeToken.getRawType());
		
		
		job.setInputFormatClass(TextInputFormat.class);

	}
	
	private void updateIslandEvolutionObservers() {
		
		PopulationData<T> populationData = StringUtils.fromString(fsUtils.readFromFile("output/population_data"));
    	for(EvolutionObserver<? super T> observer: observers) {
    		
    		observer.populationUpdate(populationData);
    	}
	}
	
	private void updateDistributedFitnessObservers(PopulationData<T> data) {
		
		for(EvolutionObserver<? super T> observer: observers) {
    		
    		observer.populationUpdate(data);
    	}
	}
	
	private int getGenerationCount(TerminationCondition[] conditions) {
		
		int generationCount = -1;
		
		if(conditions == null || conditions.length == 0) {
			
			return -1;
		}
		
		//Get generation count if such termination condition is set;
    	for(int i = 0; i < conditions.length; i++) {
    		
    		if(conditions[i] instanceof GenerationCount) {
    			
    			GenerationCount genCountCondition = (GenerationCount)conditions[i];
    			Field[] fields = GenerationCount.class.getDeclaredFields();
    			
    			for(Field field: fields) {
    				
    				if("generationCount".equals(field.getName())) {
    					
    					field.setAccessible(true);
    					try {
							generationCount = field.getInt(genCountCondition);
						} catch (IllegalArgumentException e) {
							
							e.printStackTrace();
						} catch (IllegalAccessException e) {
							
							e.printStackTrace();
						}
    					
    					break;
    				}
    			}
    		}
    	}
    	
    	return generationCount;
	}	
	 
	 public List<PopulationWritable<T>> splitPopulation(PopulationWritable<T> population, int numberOfSplits) {
		 
		 List<PopulationWritable<T>> subpopulations = new ArrayList<PopulationWritable<T>>(numberOfSplits);
		 
		 int splitSize = population.getPopulation().size() / numberOfSplits;
		 
		 //Create the first numberOfSplits - 1 subpopulations;
		 for(int i = 0; i < numberOfSplits; i++) {
			 
			 subpopulations.add(new PopulationWritable<T>());
			 subpopulations.get(i).setData(population.getData());
			 for(int j = 0; j < splitSize; j++) {
				 
				 subpopulations.get(i).getPopulation().add(population.getPopulation().get(i * splitSize + j));
			 }
		 }
		 
		 //Create the last subpopulation;
		 //This is created separately because the populationSize might not divide exactly to the numberOfSplits;
		 int processedCandidates = numberOfSplits * splitSize;
		 for(int i = processedCandidates; i < population.getPopulation().size(); i++) {
			 
			 subpopulations.get(i - processedCandidates).getPopulation().add(population.getPopulation().get(i));
		 }
 		 
		 return subpopulations;
	 }
	 
	 public void writeSubpopulationsToFiles(List<PopulationWritable<T>> subpopulations, String inputFolder) {
		 
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
		 
		 for(int i = 0; i < subpopulations.size(); i++) {
			 
			subpopulations.get(i).putDataValue(Constants.POPULATION_INDEX_PARAMETER, i);
			fsUtils.writeToFile(subDirectory.getPath() + "/subpopulation_" + i, StringUtils.toString(subpopulations.get(i)));
		 }
	 }
	 
	 public void writeIndividualsToFile(List<T> population, String outputFile) {
		 
		 if(population == null || population.size() == 0) {
			 
			 return;
		 }
		 
		 StringBuilder builder = new StringBuilder("");
		 for(int i = 0; i < population.size(); i++) {
			 
			 builder.append(StringUtils.toString(population.get(i)) + "\n");
		 }
		 
		 fsUtils.writeToFile(outputFile, builder.toString());
		 
	 }
	 
	 @SuppressWarnings("unchecked")
	 public List<EvaluatedCandidate<T>> readAndMergePopulationPartitionsFromFiles(String directory) {
		
		 List<EvaluatedCandidate<T>> population = new ArrayList<EvaluatedCandidate<T>>();
		 
		 try {
			 
			Path path = new Path(directory);
			FileSystem fs = FileSystem.get(new Configuration());
			RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(path, false);
			LocatedFileStatus fileStatus = null;
			String data = null;
			
			while(fileIterator.hasNext()) {
				
				fileStatus = fileIterator.next();
				
				if(fileStatus.isFile() && fileStatus.getPath().getName().contains("partition")) {
					
					data = fsUtils.readFromFile(fileStatus.getPath().toString());
					population.addAll((List<EvaluatedCandidate<T>>)StringUtils.fromString(data));
				}
			}
		 }	
		 catch (IOException e) {
				
			 e.printStackTrace();
		}
 
		return population;
	 }
	 
	 @SuppressWarnings("unchecked")
	 public List<PopulationWritable<T>> readSubpopulationsFromFiles(String inputFolder) {
		 
		 List<PopulationWritable<T>> subpopulations = new ArrayList<PopulationWritable<T>>();

		 try {
			 
			Path path = new Path(inputFolder);
			FileSystem fs = FileSystem.get(new Configuration());
			RemoteIterator<LocatedFileStatus> fileIterator = fs.listFiles(path, false);
			LocatedFileStatus fileStatus = null;
			String data = null;
			
			while(fileIterator.hasNext()) {
				
				fileStatus = fileIterator.next();
				if(fileStatus.isFile() && fileStatus.getPath().getName().contains("subpopulation")) {
					
					data = fsUtils.readFromFile(fileStatus.getPath().toString());
					subpopulations.add((PopulationWritable<T>)StringUtils.fromString(data));
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		 
		 return subpopulations;
	 }
	 
	 public int getNumberOfSlaves(String confDirectory) throws IOException {
		   
    	BufferedReader reader = new BufferedReader(new FileReader(new File(confDirectory + "/slaves")));
    	int slaves = 0;
    	
    	while(reader.readLine() != null) {
    		
    		slaves++;
    	}
    	
    	reader.close();
    	
    	return slaves;
    }
	 
	 private boolean checkTerminationStatus() {
		 
		 String status = fsUtils.readFromFile("output/should_terminate");
		 
		 return Constants.TRUE.equals(status);
	 }
	 
	 private void listDirectory(String directory) {
		
		 try {
			
			System.out.println("Listing directory: " + directory);
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus[] status = fs.listStatus(new Path(directory));
			 
			for(int i = 0; i < status.length; i++) {
					
				System.out.println(status[i].getPath().getName());
			}
		 } catch(Exception e) {
				
				e.printStackTrace();
			}
		
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
	 
	 public void displayTime(String message, long elapsedTime) {
		 
		 double seconds = (double) elapsedTime / 1000;
		 System.out.println(message + ": "  + seconds);
	 }

	public EvolutionaryOperator<T> getEvolutionScheme() {
		return evolutionScheme;
	}

	public FitnessEvaluator<? super T> getFitnessEvaluator() {
		return fitnessEvaluator;
	}

	public SelectionStrategy<? super T> getSelectionStrategy() {
		return selectionStrategy;
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
	public void addEvolutionObserver(EvolutionObserver<? super T> observer) {
		
		this.observers.add(observer);
	}

	@Override
	public void removeEvolutionObserver(EvolutionObserver<? super T> observer) {
		
		this.observers.remove(observer);
	}

	@Override
	public List<TerminationCondition> getSatisfiedTerminationConditions() {
		// TODO Auto-generated method stub
		return null;
	}
	 
	 
}
