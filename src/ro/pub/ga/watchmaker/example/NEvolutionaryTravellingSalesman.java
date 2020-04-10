/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ro.pub.ga.watchmaker.example;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.ga.watchmaker.travellingsalesman.DistanceLookup;
import org.apache.mahout.ga.watchmaker.travellingsalesman.ProgressListener;
import org.apache.mahout.ga.watchmaker.travellingsalesman.TravellingSalesmanStrategy;
import org.uncommons.maths.random.PoissonGenerator;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvolutionEngine;
import org.uncommons.watchmaker.framework.EvolutionObserver;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.PopulationData;
import org.uncommons.watchmaker.framework.SelectionStrategy;
import org.uncommons.watchmaker.framework.operators.EvolutionPipeline;
import org.uncommons.watchmaker.framework.termination.GenerationCount;

import ro.pub.ga.watchmaker.core.DistributedEvolutionEngine;
import ro.pub.ga.watchmaker.core.DistributedEvolutionEngine.EvolutionType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ApplicationType;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ElitismType;
import ro.pub.ga.watchmaker.utils.ArrayListOrderCrossover;
import ro.pub.ga.watchmaker.utils.ArrayListOrderMutation;
import ro.pub.ga.watchmaker.utils.ArrayListPermutationFactory;

import com.google.common.base.Preconditions;

/**
 * Evolutionary algorithm for finding (approximate) solutions to the travelling salesman problem.
 * 
 * 
 * <br>
 * The original code is from <b>the Watchmaker project</b> (https://watchmaker.dev.java.net/).<br>
 * Modified to use Mahout whenever requested.
 */
public class NEvolutionaryTravellingSalesman implements TravellingSalesmanStrategy {
  
  private final DistanceLookup distances;
  
  private final SelectionStrategy<? super List<String>> selectionStrategy;
  
  private final int populationSize;
  
  private final int eliteCount;
  
  private final int generationCount;
  
  private final boolean crossover;
  
  private final boolean mutation;
  
  private final boolean mahout;
  
  private final EvolutionType evolutionType;
  
  private  String configurationFolder;
  
  private  int slaves;
  
  /**
   * Creates an evolutionary Travelling Salesman solver with the specified configuration.
   * 
   * @param distances
   *          Information about the distances between cities.
   * @param selectionStrategy
   *          The selection implementation to use for the evolutionary algorithm.
   * @param populationSize
   *          The number of candidates in the population of evolved routes.
   * @param eliteCount
   *          The number of candidates to preserve via elitism at each generation.
   * @param generationCount
   *          The number of iterations of evolution to perform.
   * @param crossover
   *          Whether or not to use a cross-over operator in the evolution.
   * @param mutation
   *          Whether or not to use a mutation operator in the evolution.
   * @param mahout
   *          Whether or not to use Mahout for evaluation.
   */
  public NEvolutionaryTravellingSalesman(DistanceLookup distances,
                                        SelectionStrategy<? super List<String>> selectionStrategy,
                                        int populationSize,
                                        int eliteCount,
                                        int generationCount,
                                        boolean crossover,
                                        boolean mutation,
                                        boolean mahout,
                                        EvolutionType evolutionType,
                                        String configurationFolder) {
    Preconditions.checkArgument(eliteCount >= 0 && eliteCount < populationSize,
        "Elite count must be non-zero and less than population size.");
    Preconditions.checkArgument(crossover || mutation, "At least one of cross-over or mutation must be selected.");
    this.distances = distances;
    this.selectionStrategy = selectionStrategy;
    this.populationSize = populationSize;
    this.eliteCount = eliteCount;
    this.generationCount = generationCount;
    this.crossover = crossover;
    this.mutation = mutation;
    this.mahout = mahout;
    this.evolutionType = evolutionType;
    this.configurationFolder = configurationFolder;
  }
  
  public NEvolutionaryTravellingSalesman(DistanceLookup distances,
          SelectionStrategy<? super List<String>> selectionStrategy,
          int populationSize,
          int eliteCount,
          int generationCount,
          boolean crossover,
          boolean mutation,
          boolean mahout,
          EvolutionType evolutionType,
          int slaves) {
	  
		Preconditions.checkArgument(eliteCount >= 0 && eliteCount < populationSize,
		"Elite count must be non-zero and less than population size.");
		Preconditions.checkArgument(crossover || mutation, "At least one of cross-over or mutation must be selected.");
		this.distances = distances;
		this.selectionStrategy = selectionStrategy;
		this.populationSize = populationSize;
		this.eliteCount = eliteCount;
		this.generationCount = generationCount;
		this.crossover = crossover;
		this.mutation = mutation;
		this.mahout = mahout;
		this.evolutionType = evolutionType;
		this.slaves = slaves;
}
  
  @Override
  public String getDescription() {
    String selectionName = selectionStrategy.getClass().getSimpleName();
    return (mahout ? "Mahout " : "") + "Evolution (pop: " + populationSize + ", gen: " + generationCount
           + ", elite: " + eliteCount + ", " + selectionName + ')';
  }
  
  /**
   * Calculates the shortest route using a generational evolutionary algorithm with a single ordered mutation
   * operator and truncation selection.
   * 
   * @param cities
   *          The list of destinations, each of which must be visited once.
   * @param progressListener
   *          Call-back for receiving the status of the algorithm as it progresses.
   * @return The (approximate) shortest route that visits each of the specified cities once.
   */
  @Override
  public List<String> calculateShortestRoute(Collection<String> cities,
                                             final ProgressListener progressListener) {
    Random rng = RandomUtils.getRandom();
    
    // Set-up evolution pipeline (cross-over followed by mutation).
    ArrayList<EvolutionaryOperator<ArrayList<String>>> operators = new ArrayList<EvolutionaryOperator<ArrayList<String>>>(2);
    if (crossover) {
      operators.add(new ArrayListOrderCrossover<String>());
    }
    if (mutation) {
      operators.add(new ArrayListOrderMutation<String>(new PoissonGenerator(1.5, rng), new PoissonGenerator(1.5, rng)));
    }
    
    EvolutionaryOperator<ArrayList<String>> pipeline = new EvolutionPipeline<ArrayList<String>>(operators);
    
    CandidateFactory<ArrayList<String>> candidateFactory = new ArrayListPermutationFactory<String>(
        new ArrayList<String>(cities));
    EvolutionEngine<ArrayList<String>> engine = getEngine(candidateFactory, pipeline, rng);
    
    engine.addEvolutionObserver(new EvolutionObserver<List<String>>() {

		@Override
		public void populationUpdate(PopulationData<? extends List<String>> data) {
			
			System.out.println("\nGeneration: " + data.getGenerationNumber() + ", Best fitness: " 
					+ data.getBestCandidateFitness() 
					+ ", MeanFitness: " + data.getMeanFitness() + "\n");
		}
	});
    
    return engine.evolve(populationSize, eliteCount,new GenerationCount(generationCount));
  }
  
  private EvolutionEngine<ArrayList<String>> getEngine(CandidateFactory<ArrayList<String>> candidateFactory,
                                                  EvolutionaryOperator<ArrayList<String>> pipeline,
                                                  Random rng) {
	  //"/home/daniel/Desktop/Licenta/hadoop-2.6.0/etc/hadoop"
      
	  // Oana - comentat pt a testa engine-ul meu
	  //return new DistributedEvolutionEngine<ArrayList<String>>(candidateFactory, pipeline, new NRouteEvaluator(distances),
      //        selectionStrategy, rng, evolutionType, true, slaves);
	  //System.out.println("[Oana] getting engine ");
      
	  // TODO foloseste evolution type din hybrid nu din DEE
	  //return new DistributedHybridEvolutionEngine<ArrayList<String>>(candidateFactory, pipeline, new NRouteEvaluator(distances),
		//	  selectionStrategy, rng, evolutionType, true, slaves);
	  return new DistributedHybridEvolutionEngine<ArrayList<String>>(candidateFactory, pipeline, new NRouteEvaluator(distances),
              selectionStrategy, rng, 
              DistributedHybridEvolutionEngine.EvolutionType.MasterSlaveIslands,
              ElitismType.All,
              ApplicationType.Unknown,
              true,
              slaves);

  }
}
