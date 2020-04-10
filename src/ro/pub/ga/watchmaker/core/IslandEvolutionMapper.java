package ro.pub.ga.watchmaker.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.GenerationalEvolutionEngine;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;
import org.uncommons.watchmaker.framework.SteadyStateEvolutionEngine;
import org.uncommons.watchmaker.framework.TerminationCondition;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.jss.ScheduleFactory;
import ro.pub.ga.watchmaker.jss.ScheduleItem;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

import com.google.common.base.Preconditions;

public class IslandEvolutionMapper<T> extends Mapper<LongWritable, Text, LongWritable, PopulationWritable<EvaluatedCandidate<T>>>{
	
	private CandidateFactory<T> candidateFactory;
	private EvolutionaryOperator<T> evolutionScheme;
	private FitnessEvaluator<? super T> fitnessEvaluator;
	private SelectionStrategy<? super T> selectionStrategy;
	private List<TerminationCondition> terminationConditions;
	private Random rng;
	private int generationCount;
	private int eliteCount;
	private Boolean doMigration;
	private Integer migrationCount;
	
	private static final LongWritable one = new LongWritable(1); 
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, LongWritable, PopulationWritable<EvaluatedCandidate<T>>>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		//Get the candidateFactory attribute;
		String stringifiedProperty = conf.get(Constants.CANDIDATE_FACTORY_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'CANDIDATE_FACTORY_PROPERTY' job parameter not found");
		this.candidateFactory = StringUtils.fromString(stringifiedProperty);
		
		//Get the evolutionScheme attribute;
		stringifiedProperty = conf.get(Constants.EVOLUTION_SCHEME_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'EVOLUTION_SCHEME_PROPERTY' job parameter not found");
		this.evolutionScheme = StringUtils.fromString(stringifiedProperty);
		
		//Get the fitnessEvaluator attribute;
		stringifiedProperty = conf.get(Constants.FITNESS_EVALUATOR_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'FITNESS_EVALUATOR_PROPERTY' job parameter not found");
		this.fitnessEvaluator = StringUtils.fromString(stringifiedProperty);
		
		//Get the selectionStrategy attribute;
		stringifiedProperty = conf.get(Constants.SELECTION_STRATEGY_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'SELECTION_STRATEGY_PROPERTY' job parameter not found");
		this.selectionStrategy = StringUtils.fromString(stringifiedProperty);
		
		//Get the termination conditions;
		stringifiedProperty = conf.get(Constants.TERMINATION_CONDITIONS);
		Preconditions.checkArgument(stringifiedProperty != null, "'TERMINATION_CONDITIONS' job parameter not found");
		this.terminationConditions = new ArrayList<TerminationCondition>((List<TerminationCondition>)StringUtils.fromString(stringifiedProperty));
		
		//Get the rng attribute;
		stringifiedProperty = conf.get(Constants.RANDOM_GENERATOR_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'RANDOM_GENERATOR_PROPERTY' job parameter not found");
		this.rng = StringUtils.fromString(stringifiedProperty);
		
		//Get the generationCount attribute;
		this.generationCount = conf.getInt(Constants.GENERATION_COUNT_PROPERTY, -1);
		Preconditions.checkArgument(this.generationCount > 0, "'GENERATION_COUNT_PROPERTY' job parameter not found");
		
		//Get the eliteCounte attribute; it is optional;
		this.eliteCount = conf.getInt(Constants.ELITE_COUNT_PROPERTY, 0);
		
		//System.out.println(7);
		this.doMigration = conf.getBoolean(Constants.MIGRATION_PROPERTY, false);
		if(doMigration) {
			
			this.migrationCount = conf.getInt(Constants.MIGRATION_COUNT_PROPERTY, 0);
		}
		
		super.setup(context);
	}
	
	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, LongWritable, PopulationWritable<EvaluatedCandidate<T>>>.Context context)
			throws IOException, InterruptedException {
		
		//Extract the population from the input value
		PopulationWritable<T> population = StringUtils.fromString(value.toString());
		Integer populationIndex = (Integer)population.getDataValue(Constants.POPULATION_INDEX_PARAMETER);
		System.out.println("Elite count: " + eliteCount);
		
		//Create a single threaded GenerationalEvolutionEngine
		//SteadyStateEvolutionEngine<T> engine = new SteadyStateEvolutionEngine<>(candidateFactory, evolutionScheme, fitnessEvaluator, selectionStrategy, 2, true, rng);
		GenerationalEvolutionEngine<T> engine = new GenerationalEvolutionEngine<T>(candidateFactory, evolutionScheme, fitnessEvaluator, selectionStrategy, rng);
		engine.setSingleThreaded(true);
		engine.addEvolutionObserver(new EvolutionObserver<T>() {

			
			public void populationUpdate(PopulationData<? extends T> data) {
				
				System.out.println("Generation number: " + data.getGenerationNumber() + ", Best fitness: " + data.getBestCandidateFitness() + ", Mean fitness: " + data.getMeanFitness());
			}
		});
		
		//Evolve the received subpopulation
		terminationConditions.add(new GenerationCount(generationCount));
		PopulationWritable<EvaluatedCandidate<T>> evolvedPopulation = new PopulationWritable<EvaluatedCandidate<T>>(
				engine.evolvePopulation(
						population.getPopulation().size(), 
						eliteCount, population.getPopulation(), 
						terminationConditions.toArray(new TerminationCondition[terminationConditions.size()])
				)
		);
		evolvedPopulation.putDataValue(Constants.POPULATION_INDEX_PARAMETER, populationIndex);
		
		//Migration
		if(doMigration) {
			
			selectMigrators(evolvedPopulation);
		}
		
		context.getConfiguration().setBoolean(Constants.MIGRATION_PROPERTY, doMigration);
		context.getConfiguration().set(Constants.RANDOM_GENERATOR_PROPERTY, StringUtils.toString(rng));
		context.getConfiguration().setBoolean(Constants.NATURAL_EVOLUTION, fitnessEvaluator.isNatural());
		
		//Pass the population further to the next step
		context.write(one, evolvedPopulation);
	}
	
	public void selectMigrators(PopulationWritable<EvaluatedCandidate<T>> population) {
		
		List<EvaluatedCandidate<T>> migrators = new ArrayList<EvaluatedCandidate<T>>();
		
		int counter = migrationCount;
		int index;
		while(counter > 0) {
			
			index = rng.nextInt(population.getPopulation().size());
			migrators.add(population.getPopulation().remove(index));
			counter--;
		}
		
		population.putDataValue(Constants.INDIVIDUALS_TO_MIGRATE, migrators);
	}
}
