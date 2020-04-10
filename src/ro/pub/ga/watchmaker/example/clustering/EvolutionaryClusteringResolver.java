package ro.pub.ga.watchmaker.example.clustering;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.uncommons.maths.random.PoissonGenerator;
import org.uncommons.maths.random.Probability;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;
import org.uncommons.watchmaker.framework.operators.EvolutionPipeline;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ApplicationType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.EvolutionType;

import com.google.common.base.Preconditions;

public class EvolutionaryClusteringResolver {

	private final FitnessEvaluator<List<Float>> evaluator;

	private final SelectionStrategy<? super List<Float>> selectionStrategy;

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

	private final int nrOfCenters;

	public EvolutionaryClusteringResolver(FitnessEvaluator<List<Float>> evaluator,
			SelectionStrategy<? super List<Float>> selectionStrategy,
					int populationSize,
					int eliteCount,
					ElitismType elitismType,
					int generationCount,
					int nrOfCenters,
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
		this.generationCount = generationCount;
		this.nrOfCenters = nrOfCenters;
		this.crossover = crossover;
		this.mutation = mutation;
		this.mahout = mahout;
		this.evolutionType = evolutionType;
		this.configurationFolder = configurationFolder;
	}

	public EvolutionaryClusteringResolver(FitnessEvaluator<List<Float>> evaluator,
			SelectionStrategy<? super List<Float>> selectionStrategy,
					int populationSize,
					int eliteCount,
					ElitismType elitismType,
					int generationCount,
					int nrOfCenters,
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
		this.generationCount = generationCount;
		this.nrOfCenters = nrOfCenters;
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
	    	", " + selectionName +')';
	  }
	
	public List<Float> calculateCenters(List<ArrayList<Float>> points,
			final ProgressListener progressListener) {
		
		Random rng = RandomUtils.getRandom();

		// Set-up evolution pipeline (cross-over followed by mutation).
		ArrayList<EvolutionaryOperator<ArrayList<Float>>> operators =
				new ArrayList<EvolutionaryOperator<ArrayList<Float>>>(2);
		if (crossover) {
			operators.add(new CenterArrayListCrossover<Float>(new Probability(1d),
					points.get(0).size()));
		}
		if (mutation) {
			operators.add(new CenterArrayListMutation<Float>(new PoissonGenerator(1.5, rng), new PoissonGenerator(1.5, rng)));
		}

		EvolutionaryOperator<ArrayList<Float>> pipeline = new EvolutionPipeline<ArrayList<Float>>(operators);
		
		CandidateFactory<ArrayList<Float>> candidateFactory = new CenterCandidateFactory<Float>(points, nrOfCenters);

		EvolutionEngine<ArrayList<Float>> engine = getEngine(candidateFactory, pipeline, rng);

		engine.addEvolutionObserver(new EvolutionObserver<List<Float>>() {

			@Override
			public void populationUpdate(PopulationData<? extends List<Float>> data) {

				System.out.println("\nGeneration: " + data.getGenerationNumber() + 
						"Population size: " + data.getPopulationSize() +
						", Best fitness: " 
						+ data.getBestCandidateFitness() 
						+ ", MeanFitness: " + data.getMeanFitness() + "\n");
			}
		});

		return engine.evolve(populationSize, eliteCount, new GenerationCount(generationCount));
	}
	
	private EvolutionEngine<ArrayList<Float>> getEngine(CandidateFactory<ArrayList<Float>> candidateFactory,
			EvolutionaryOperator<ArrayList<Float>> pipeline,
			Random rng) {
		return new DistributedHybridEvolutionEngine<ArrayList<Float>>(candidateFactory, pipeline, evaluator,
				selectionStrategy, rng, evolutionType, elitismType, ApplicationType.Clustering, true, slaves);
	}
}
