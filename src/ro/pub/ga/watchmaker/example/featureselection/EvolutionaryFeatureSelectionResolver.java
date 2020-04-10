package ro.pub.ga.watchmaker.example.featureselection;

import java.util.ArrayList;
import java.util.Random;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.uncommons.maths.binary.BitString;
import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;
import org.uncommons.watchmaker.framework.factories.BitStringFactory;
import org.uncommons.watchmaker.framework.operators.BitStringCrossover;
import org.uncommons.watchmaker.framework.operators.BitStringMutation;
import org.uncommons.watchmaker.framework.operators.EvolutionPipeline;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.example.featureselection.BitStringCrossoverOperator.CrossoverOperatorType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ApplicationType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.EvolutionType;

import com.google.common.base.Preconditions;

public class EvolutionaryFeatureSelectionResolver {

	private final FitnessEvaluator<BitString> evaluator;

	private final SelectionStrategy<? super BitString> selectionStrategy;

	private final int populationSize;

	private final int eliteCount;

	private final int generationCount;

	private final boolean crossover;

	private final boolean mutation;

	private final boolean mahout;

	private final EvolutionType evolutionType;
	
	private final ElitismType elitismType;

	private  String configurationFolder;

	private  int slaves;

	private final int totalNrOfFeatures;
	
	private BitStringCrossoverOperator.CrossoverOperatorType crossoverType;

	public EvolutionaryFeatureSelectionResolver(FitnessEvaluator<BitString> evaluator,
			SelectionStrategy<? super BitString> selectionStrategy,
					int populationSize,
					int eliteCount,
					ElitismType elitismType,
					BitStringCrossoverOperator.CrossoverOperatorType crossoverType,
					int generationCount,
					int totalNrOfFeatures,
					boolean crossover,
					boolean mutation,
					boolean mahout,
					EvolutionType evolutionType,
					String configurationFolder) {
		Preconditions.checkArgument(eliteCount >= 0 && eliteCount < populationSize,
				"Elite count must be non-zero and less than population size.");
		Preconditions.checkArgument(crossover || mutation, "At least one of cross-over or mutation must be selected.");
		this.evaluator = evaluator;
		this.selectionStrategy = selectionStrategy;
		this.populationSize = populationSize;
		this.eliteCount = eliteCount;
		this.elitismType = elitismType;
		this.crossoverType = crossoverType;
		this.generationCount = generationCount;
		this.totalNrOfFeatures = totalNrOfFeatures;
		this.crossover = crossover;
		this.mutation = mutation;
		this.mahout = mahout;
		this.evolutionType = evolutionType;
		this.configurationFolder = configurationFolder;
	}

	public EvolutionaryFeatureSelectionResolver(FitnessEvaluator<BitString> evaluator,
			SelectionStrategy<? super BitString> selectionStrategy,
					int populationSize,
					int eliteCount,
					ElitismType elitismType,
					CrossoverOperatorType crossoverType,
					int generationCount,
					int totalNrOfFeatures,
					boolean crossover,
					boolean mutation,
					boolean mahout,
					EvolutionType evolutionType,
					int slaves) {

		Preconditions.checkArgument(eliteCount >= 0 && eliteCount < populationSize,
		"Elite count must be non-zero and less than population size.");
		Preconditions.checkArgument(crossover || mutation, "At least one of cross-over or mutation must be selected.");
		this.evaluator = evaluator;
		this.selectionStrategy = selectionStrategy;
		this.populationSize = populationSize;
		this.eliteCount = eliteCount;
		this.elitismType = elitismType;
		this.crossoverType = crossoverType;
		this.generationCount = generationCount;
		this.totalNrOfFeatures = totalNrOfFeatures;
		this.crossover = crossover;
		this.mutation = mutation;
		this.mahout = mahout;
		this.evolutionType = evolutionType;
		this.slaves = slaves;
	}
	
	public String getDescription() {
	    String selectionName = selectionStrategy.getClass().getSimpleName();
	    return (mahout ? "Mahout " : "") + "Evolution (pop: " + populationSize + ", gen: "
	    	+ generationCount + ", elite: " +
	    	(elitismType == ElitismType.All ? " all" : (eliteCount + " picked " + 
	    	(elitismType == ElitismType.BestCandidates ? "best ones" : " random"))) +
	    	", " + "crossover type " + 
	    	(crossoverType == CrossoverOperatorType.AND ? "AND" :
	    		(crossoverType == CrossoverOperatorType.OR ? "OR" : 
	    			(crossoverType == CrossoverOperatorType.XOR ? "XOR" : "SinglePoint"))) + 
	    	" ,"+ selectionName +')';
	  }
	
	public BitString selectFeatures(final ProgressListener progressListener) {
		
		Random rng = RandomUtils.getRandom();

		// Set-up evolution pipeline (cross-over followed by mutation).
		ArrayList<EvolutionaryOperator<BitString>> operators =
				new ArrayList<EvolutionaryOperator<BitString>>(2);
		if (crossover) {
			operators.add(BitStringCrossoverOperator.getOperator(totalNrOfFeatures, crossoverType));
		}
		if (mutation) {
			operators.add(new BitStringMutation(new Probability(1d)));
		}

		EvolutionaryOperator<BitString> pipeline = new EvolutionPipeline<BitString>(operators);
		
		CandidateFactory<BitString> candidateFactory = new BitStringFactory(totalNrOfFeatures);

		EvolutionEngine<BitString> engine = getEngine(candidateFactory, pipeline, rng);

		engine.addEvolutionObserver(new EvolutionObserver<BitString>() {
			@Override
			public void populationUpdate(PopulationData<? extends BitString> data) {
				System.out.println("\nGeneration: " + data.getGenerationNumber() + 
						"Population size: " + data.getPopulationSize() +
						", Best fitness: " 
						+ data.getBestCandidateFitness() 
						+ ", MeanFitness: " + data.getMeanFitness() + "\n");
			}
		});
		return engine.evolve(populationSize, eliteCount, new GenerationCount(generationCount));
	}
	
	private EvolutionEngine<BitString> getEngine(CandidateFactory<BitString> candidateFactory,
			EvolutionaryOperator<BitString> pipeline,
			Random rng) {
		return new DistributedHybridEvolutionEngine<BitString>(candidateFactory, pipeline, evaluator,
				selectionStrategy, rng, evolutionType, elitismType, ApplicationType.Unknown, true, slaves);
	}
}

