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

package org.apache.mahout.ga.watchmaker;

import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.uncommons.watchmaker.framework.AbstractEvolutionEngine;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.SelectionStrategy;

import com.google.common.collect.Lists;

/** Single Threaded Evolution Engine. */
public class STEvolutionEngine<T> extends AbstractEvolutionEngine<T> {
	
	private final EvolutionaryOperator<T> evolutionScheme;
	private final FitnessEvaluator<? super T> fitnessEvaluator;
	private final SelectionStrategy<? super T> selectionStrategy;
  
  public STEvolutionEngine(CandidateFactory<T> candidateFactory,
                           EvolutionaryOperator<T> evolutionScheme,
                           FitnessEvaluator<? super T> fitnessEvaluator,
                           SelectionStrategy<? super T> selectionStrategy,
                           Random rng) {
    //super(candidateFactory, evolutionScheme, fitnessEvaluator, selectionStrategy, rng);
	 super(candidateFactory,fitnessEvaluator, rng);
	 this.evolutionScheme = evolutionScheme;
	 this.fitnessEvaluator = fitnessEvaluator;
	 this.selectionStrategy = selectionStrategy;
  }
  
  @Override
  protected List<EvaluatedCandidate<T>> evaluatePopulation(List<T> population) {
    List<Double> evaluations = Lists.newArrayList();
    STFitnessEvaluator<? super T> evaluator = (STFitnessEvaluator<? super T>) getFitnessEvaluator();
    
    evaluator.evaluate(population, evaluations);
    
    List<EvaluatedCandidate<T>> evaluatedPopulation = Lists.newArrayList();
    for (int index = 0; index < population.size(); index++) {
      evaluatedPopulation.add(new EvaluatedCandidate<T>(population.get(index), evaluations.get(index)));
    }
    
    // Sort candidates in descending order according to fitness.
    if (getFitnessEvaluator().isNatural()) { // Descending values for natural fitness.
      Collections.sort(evaluatedPopulation, Collections.reverseOrder());
    } else { // Ascending values for non-natural fitness.
      Collections.sort(evaluatedPopulation);
    }
    
    return evaluatedPopulation;
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
	protected List<EvaluatedCandidate<T>> nextEvolutionStep(
			List<EvaluatedCandidate<T>> evaluatedPopulation, int eliteCount,
			Random rng) {
		// TODO Auto-generated method stub
		return null;
	}
  
  
  
}
