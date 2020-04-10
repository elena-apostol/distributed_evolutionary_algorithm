package ro.pub.ga.watchmaker.hybrid.core;

import java.io.IOException;
import java.util.ArrayList;
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
import org.uncommons.watchmaker.framework.TerminationCondition;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.core.Constants;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

public class MicroMacroIslandsMapper<T> extends Mapper<LongWritable, Text, LongWritable, PopulationWritable<EvaluatedCandidate<T>>> {

	private CandidateFactory<T> candidateFactory;
	private EvolutionaryOperator<T> evolutionScheme;
	private FitnessEvaluator<? super T> fitnessEvaluator;
	private SelectionStrategy<? super T> selectionStrategy;
	private List<TerminationCondition> terminationConditions;
	private Random rng;
	private int generationCount;
	private int eliteCount;
	private Boolean doMigration;
	private Integer migrationCountPercent;
	private int numberOfMacropopulations;
	private ElitismType elitismType;
	private int currentEpoch;
	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, LongWritable, PopulationWritable<EvaluatedCandidate<T>>>.Context context)
			throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		this.candidateFactory = StringUtils.fromString(conf.get(Constants.CANDIDATE_FACTORY_PROPERTY));
		this.evolutionScheme = StringUtils.fromString(conf.get(Constants.EVOLUTION_SCHEME_PROPERTY));
		this.fitnessEvaluator = StringUtils.fromString(conf.get(Constants.FITNESS_EVALUATOR_PROPERTY));
		this.selectionStrategy = StringUtils.fromString(conf.get(Constants.SELECTION_STRATEGY_PROPERTY));
		this.terminationConditions = new ArrayList<TerminationCondition>((List<TerminationCondition>)StringUtils.fromString(conf.get(Constants.TERMINATION_CONDITIONS)));
		this.rng = StringUtils.fromString(conf.get(Constants.RANDOM_GENERATOR_PROPERTY));
		this.generationCount = conf.getInt(Constants.GENERATION_COUNT_PROPERTY, -1);
		this.eliteCount = conf.getInt(Constants.ELITE_COUNT_PROPERTY, 0);
		if (eliteCount != -1) {
			elitismType = StringUtils.fromString(conf.get(Constants.ELITISM_TYPE));
		}
		this.doMigration = conf.getBoolean(Constants.MIGRATION_PROPERTY, false);
		if(doMigration) {
			this.migrationCountPercent = conf.getInt(Constants.MIGRATION_COUNT_PROPERTY, 0);
		}
		this.numberOfMacropopulations = conf.getInt(Constants.NUM_MACROISLANDS, -1);
		this.currentEpoch = conf.getInt(Constants.CURRENT_EPOCH, -1);
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
		Integer micropopulationIndex = (Integer)population.getDataValue(Constants.MICROPOPULATION_INDEX);
		Integer macropopulationIndex = (Integer)population.getDataValue(Constants.MACROPOPULATION_INDEX);
		
		java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
		System.out.println("[Oana][Mapper]" +localMachine.getHostName() + " population micro " + micropopulationIndex + 
				" macro " + macropopulationIndex + " nr of candidates " + population.getPopulation().size());
		
		//Create a single threaded GenerationalEvolutionEngine
		GenerationalEvolutionEngine<T> engine = new GenerationalEvolutionEngine<T>(candidateFactory, evolutionScheme, fitnessEvaluator, selectionStrategy, rng);
		engine.setSingleThreaded(true);
		engine.addEvolutionObserver(new EvolutionObserver<T>() {
			public void populationUpdate(PopulationData<? extends T> data) {
				System.out.println("[Oana][Mapper] Generation number: " + data.getGenerationNumber() + ", Best fitness: " + data.getBestCandidateFitness() + ", Mean fitness: " + data.getMeanFitness());
			}
		});
		
		eliteCount = Math.max(eliteCount, 0);
		
		if (elitismType == ElitismType.All) {
			// This value must be non-negative and less than the population size.
			eliteCount = population.getPopulation().size() - 1;
		}
		
		System.out.println("[Oana][Mapper] " + " epoch :" + currentEpoch +" Elite count: " + eliteCount + " population size " + population.getPopulation().size());
		StringBuilder sb = new StringBuilder();
		sb.append("[Mapper][epoch " + currentEpoch +"] before evolve micropo size " + population.getPopulation().size());
		
		//Evolve the received subpopulation
		terminationConditions.add(new GenerationCount(generationCount));
		List<EvaluatedCandidate<T>> evolvedCandidates = engine.evolvePopulation(
				population.getPopulation().size() + eliteCount, 
				eliteCount, population.getPopulation(), 
				terminationConditions.toArray(new TerminationCondition[terminationConditions.size()])
		);
		
		PopulationWritable<EvaluatedCandidate<T>> evolvedPopulation = 
			new PopulationWritable<EvaluatedCandidate<T>>(evolvedCandidates);
		
		evolvedPopulation.putDataValue(Constants.MICROPOPULATION_INDEX, micropopulationIndex);
		evolvedPopulation.putDataValue(Constants.MACROPOPULATION_INDEX, macropopulationIndex);
		
		sb.append("; After evolve size = " + evolvedPopulation.getPopulation().size());
		System.out.println(sb.toString());
		
		// Migration
		// migrators number = migrationCount% of micropopulation size
		if(doMigration) {
			int migrateCount = migrationCountPercent * evolvedPopulation.getPopulation().size() / 100; 
			// Half of migrators will migrate to a new micropopulation
			int migrateToMicropopulationCount = migrateCount / 2;
			if (migrateToMicropopulationCount > 0) {
				evolvedPopulation.putDataValue(Constants.INDIVIDUALS_TO_MIGRATE,
						selectRandomMigrators(evolvedPopulation,migrateToMicropopulationCount));
				System.out.println("[Oana][Mapper] Migrating to new micropopulation: " + migrateToMicropopulationCount);
			}
			
			// Half of migrators will migrate to a new macropopulation
			int migrateToMacropopulationCount = migrateCount - migrateToMicropopulationCount;
			int nextMacropopulationIndex = (macropopulationIndex + 1) % numberOfMacropopulations;
			if (migrateToMacropopulationCount > 0) {
				System.out.println("[Oana][Mapper] Migrating to new macropopulation: " + migrateToMicropopulationCount);
				PopulationWritable<EvaluatedCandidate<T>> migrators = new PopulationWritable<EvaluatedCandidate<T>>(
						selectRandomMigrators(evolvedPopulation, migrateToMacropopulationCount));
				migrators.putDataValue(Constants.MICROPOPULATION_INDEX, Constants.MIGRATOR_MICROPOPULATION_INDEX);
				context.write(new LongWritable(nextMacropopulationIndex), migrators);
			}
		}
		context.write(new LongWritable(macropopulationIndex), evolvedPopulation);
	}
	
	public List<EvaluatedCandidate<T>> selectRandomMigrators(
			PopulationWritable<EvaluatedCandidate<T>> population,
			int count) {
		
		List<EvaluatedCandidate<T>> migrators = new ArrayList<EvaluatedCandidate<T>>();
		int random;
		while(count > 0) {
			random = rng.nextInt(population.getPopulation().size());
			migrators.add(population.getPopulation().remove(random));
			count--;
		}
		return migrators;
	}
}
