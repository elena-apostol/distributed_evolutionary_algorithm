package ro.pub.ga.watchmaker.hybrid.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionUtils;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;

import ro.pub.ga.watchmaker.core.Constants;
import ro.pub.ga.watchmaker.example.NRouteEvaluator;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.utils.AssignedCandidateWritable;
import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;
import ro.pub.ga.watchmaker.utils.FSUtils;

public class MasterSlaveIslandsReducer<T> extends Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text> {

	private static final boolean RANDOM_MIGRATION_AND_FILLING = true;
	
	private SelectionStrategy<T> selectionStrategy;
	private FitnessEvaluator<T> fitnessEvaluator;
	private EvolutionaryOperator<T> evolutionaryOperator;
	private Random rng;
	private int generationCount;
	private long startTime;
	private int eliteCount;
	private boolean doMigration;
	private int migrationCount;
	private ElitismType elitismType;
	
	private int numberOfSubpopulations;
	private int slaveNumber;
	
	private Long subpopulationIndex;
	
	private FSUtils fsUtils;
	private String outpath;

	@Override
	protected void setup(
			Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text>.Context context)
	throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		
		java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
		System.out.println("[Reducer " + localMachine.getHostName() + "]");
		
		selectionStrategy = StringUtils.fromString(conf.get(Constants.SELECTION_STRATEGY_PROPERTY));
		fitnessEvaluator = StringUtils.fromString(conf.get(Constants.FITNESS_EVALUATOR_PROPERTY));
		evolutionaryOperator = StringUtils.fromString(conf.get(Constants.EVOLUTION_SCHEME_PROPERTY));
		rng = StringUtils.fromString(conf.get(Constants.RANDOM_GENERATOR_PROPERTY));
		
		generationCount = conf.getInt(Constants.GENERATION_COUNT_PROPERTY, 0);
		startTime = conf.getLong(Constants.START_TIME, 0);
		eliteCount = conf.getInt(Constants.ELITE_COUNT_PROPERTY, 0);
		if (eliteCount != -1) {
			elitismType = StringUtils.fromString(conf.get(Constants.ELITISM_TYPE));
		}
		slaveNumber = conf.getInt(Constants.NUM_SLAVES, 1);
		numberOfSubpopulations = conf.getInt(Constants.NUM_SUBPOPULATIONS, 1);
		
		doMigration = conf.getBoolean(Constants.MIGRATION_PROPERTY, false);
		if(doMigration) {
			migrationCount = conf.getInt(Constants.MIGRATION_COUNT_PROPERTY, 0);
		}
		
		fsUtils = new FSUtils(FileSystem.get(conf));
		outpath = conf.get("mapreduce.output.fileoutputformat.outputdir");
	}


	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<EvaluatedCandidateWritable<T>> arg1,
			Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text>.Context arg2)
	throws IOException, InterruptedException {
		
		subpopulationIndex = arg0.get();

		List<EvaluatedCandidate<T>> subpopulation = new ArrayList<EvaluatedCandidate<T>>();
		Iterator<EvaluatedCandidateWritable<T>> iterator = arg1.iterator();
		EvaluatedCandidateWritable<T> crtCandidate;

		List<EvaluatedCandidate<T>> newSubpopulation = new ArrayList<EvaluatedCandidate<T>>();
		
		while(iterator.hasNext()) {
			crtCandidate = iterator.next();
			subpopulation.add(new EvaluatedCandidate<T>(crtCandidate.getCandidate(), crtCandidate.getFitness()));
		}
		
		System.out.println("[Oana] [Generation " + generationCount + " Reducer - dim subpopulatie " + subpopulationIndex + " = " + subpopulation.size());
		
		int parentsNumber = Math.max(subpopulation.size() / 2, 2);

		Collections.sort(subpopulation);
		if (fitnessEvaluator.isNatural())
			Collections.reverse(subpopulation);
		
		//Select the parents in order to generate the offsprings;
		List<T> parents = selectionStrategy.select(subpopulation, fitnessEvaluator.isNatural(), parentsNumber, rng);

		//Recombine/Mutate the parents to obtain the offsprings;
		List<T> offsprings = evolutionaryOperator.apply(parents, rng);

		//Evaluate the offprings
		List<EvaluatedCandidate<T>> evaluatedOffsprings = new ArrayList<EvaluatedCandidate<T>>(offsprings.size());
		System.out.println("[Oana] for subpop " + subpopulationIndex + " offsprings:");
		for(int i = 0; i < offsprings.size(); i++) {
			System.out.println("\t[Oana] ] " + offsprings.get(i).toString());
			evaluatedOffsprings.add(
					new EvaluatedCandidate<T>(
							offsprings.get(i), 
							fitnessEvaluator.getFitness(offsprings.get(i), offsprings)
					)
			);
		}
		newSubpopulation.addAll(evaluatedOffsprings);
		
		// Elitism
		// eliteCount represents eliteCount% individuals from population that are copied to the next generation
		if (elitismType == ElitismType.All) {
			// se pastreaza tot
			newSubpopulation.addAll(subpopulation);
		} else if (elitismType == ElitismType.BestCandidates) {
			Collections.sort(subpopulation);
			if(fitnessEvaluator.isNatural()) {
				// fitness score mai mare => mai buni => iau de la final
				newSubpopulation.addAll(subpopulation.subList(
						subpopulation.size() - eliteCount - 1,subpopulation.size() - 1));
			} else {
				// ii iau pe primii eliteCount
				newSubpopulation.addAll(subpopulation.subList(0, Math.min(eliteCount, subpopulation.size() - 1)));
			}
		} else if (elitismType == ElitismType.RandomCandidates) {
			// preia random eliteCount
			for(int i = 0; i < eliteCount; i++) {
				int index = rng.nextInt(subpopulation.size());
				newSubpopulation.add(subpopulation.get(index));
			}
		} else {
			// just to be sure
			newSubpopulation.addAll(subpopulation);
		}
		
		// migrationCount - la suta din dim pop
		migrationCount = migrationCount * newSubpopulation.size() / 100;
		
		System.out.println("[Generation " + generationCount + " Reducer - dim NEW subpopulatie " + subpopulationIndex + " = " + newSubpopulation.size());
		
		if (fitnessEvaluator.isNatural()) {
			Collections.sort(newSubpopulation, Collections.reverseOrder());
		} else {
			Collections.sort(newSubpopulation);
		}
		
		System.out.println("Best Candidate from subpop " + subpopulationIndex + " has fitness " + newSubpopulation.get(0).getFitness());
		writeSubpopulationToFiles(newSubpopulation, newSubpopulation.get(0));
	}

	// scrie datele despre subpopulatie in fisierele corespunzatoare
	public void writeSubpopulationToFiles(List<EvaluatedCandidate<T>> subpopulation, EvaluatedCandidate<T> bestCandidate) {
		fsUtils.mkdir(outpath, false);
		// population data ar trebui sa nu ia in considerare migrarile
		writeBestCandidateToFile(new EvaluatedCandidate<T>(bestCandidate.getCandidate(), bestCandidate.getFitness()));
		writeEvolvedSubpopulationToFile(subpopulation);
		// does the migration too
		List<AssignedCandidateWritable<T>> assignedCandidates = assignCandidates(subpopulation);
		writeSubpopulationToFile(assignedCandidates);
	}

	public List<AssignedCandidateWritable<T>> assignCandidates(List<EvaluatedCandidate<T>> subpopulation) {
		List<AssignedCandidateWritable<T>> assignedCandidates = new ArrayList<AssignedCandidateWritable<T>>();
		if (doMigration) {
			System.out.println("Reducer doing migration of "+ migrationCount + " individuals from " +
					subpopulationIndex + " to " + ((subpopulationIndex + 1) % numberOfSubpopulations) + 
					" random? : " + RANDOM_MIGRATION_AND_FILLING);
			// Migration done in ring - TODO more ways :-?
			// check if we choose the individuals random or by selection:
			if (RANDOM_MIGRATION_AND_FILLING) {
				for (int i = 0; i < migrationCount; i++) {
						if (subpopulation.size() >= 0) {
							System.out.println("Reducer - subpopulation size: " + subpopulation.size());
							int index = rng.nextInt(subpopulation.size());
							AssignedCandidateWritable<T> assignedCandidate = 
								new AssignedCandidateWritable<T>(subpopulation.remove(index).getCandidate(),
										new Long((subpopulationIndex + 1) % numberOfSubpopulations));
							assignedCandidates.add(assignedCandidate);
					}
				}
			} else {
				// individuals to be migrated are selected using selectionStrategy
				List<T> migratedCandidates = selectionStrategy.select(subpopulation, fitnessEvaluator.isNatural(), migrationCount, rng);
				for (int i = 0; i < migrationCount; i++) {
					T candidate = migratedCandidates.get(i);
					subpopulation.remove(candidate);
					AssignedCandidateWritable<T> assignedCandidate = new AssignedCandidateWritable<T>(
							candidate, new Long((subpopulationIndex + 1) % numberOfSubpopulations));
					assignedCandidates.add(assignedCandidate);
				}
			}
			
		}
		for (EvaluatedCandidate<T> candidate : subpopulation) {
			assignedCandidates.add(new AssignedCandidateWritable<T>(candidate.getCandidate(),
					new Long(subpopulationIndex)));
		}
		return assignedCandidates;
	}
	
	// scrie subpopulatia in subpopulation_i
	public void writeSubpopulationToFile(List<AssignedCandidateWritable<T>> assignedCandidates) {
		// impart in fisere
		int nrOfFiles = numberOfSubpopulations >= slaveNumber ?
				1 : (int)Math.ceil((float)slaveNumber / (float)numberOfSubpopulations);
		System.out.println("[Reducer] writing to " + nrOfFiles + " files (slaveNR " + slaveNumber + ", nrsub" + numberOfSubpopulations);
		int splitSize = assignedCandidates.size() / nrOfFiles + 1;
		for (int i = 0; i < nrOfFiles; i++) {
			StringBuilder sb = new StringBuilder();
			for (int j = i * splitSize; j < Math.min((i+1) * splitSize, assignedCandidates.size()); j++) {
				sb.append(StringUtils.toString(assignedCandidates.get(j)) + "\n");
			}
			fsUtils.writeToFile(outpath + "/subpopulation_" + subpopulationIndex + "_" + i, sb.toString());
		}
	}

	// scrie best cadidate al subpopulatiei in best_candidate_i
	public void writeBestCandidateToFile(EvaluatedCandidate<T> bestCandidate) {
		fsUtils.writeToFile(outpath + "/best_candidate_" + subpopulationIndex, StringUtils.toString(bestCandidate));
	}

	// scrie subpopulatia evaluata
	public void writeEvolvedSubpopulationToFile(List<EvaluatedCandidate<T>> subpopulation) {
		fsUtils.writeToFile(outpath + "/evolved_population_" + subpopulationIndex, StringUtils.toString(subpopulation));
	}
	
}
