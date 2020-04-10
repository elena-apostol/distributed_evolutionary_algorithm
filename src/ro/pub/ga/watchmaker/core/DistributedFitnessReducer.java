package ro.pub.ga.watchmaker.core;

import java.io.IOException;
import java.util.ArrayList;
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
import org.uncommons.watchmaker.framework.EvolutionaryOperator;
import org.uncommons.watchmaker.framework.FitnessEvaluator;
import org.uncommons.watchmaker.framework.SelectionStrategy;

import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;
import ro.pub.ga.watchmaker.utils.FSUtils;

import com.google.common.base.Preconditions;

public class DistributedFitnessReducer<T> extends Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text> {
	
	private SelectionStrategy<T> selectionStrategy;
	private FitnessEvaluator<T> fitnessEvaluator;
	private EvolutionaryOperator<T> evolutionaryOperator;
	private Random rng;
	
	@Override
	protected void setup(
			Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		String stringifiedProperty = null;
		Configuration conf = context.getConfiguration();
		
		//Get the selectionStrategy attribute;
		stringifiedProperty = conf.get(Constants.SELECTION_STRATEGY_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'SELECTION_STRATEGY_PROPERTY' job parameter not found");
		this.selectionStrategy = StringUtils.fromString(stringifiedProperty);
		
		//Get the fitnessEvaluator attribute;
		stringifiedProperty = conf.get(Constants.FITNESS_EVALUATOR_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'FITNESS_EVALUATOR_PROPERTY' job parameter not found");
		this.fitnessEvaluator = StringUtils.fromString(stringifiedProperty);
		
		//Get the evolutionScheme attribute;
		stringifiedProperty = conf.get(Constants.EVOLUTION_SCHEME_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'EVOLUTION_SCHEME_PROPERTY' job parameter not found");
		this.evolutionaryOperator = StringUtils.fromString(stringifiedProperty);
		
		//Get the rng attribute;
		stringifiedProperty = conf.get(Constants.RANDOM_GENERATOR_PROPERTY);
		Preconditions.checkArgument(stringifiedProperty != null, "'RANDOM_GENERATOR_PROPERTY' job parameter not found");
		this.rng = StringUtils.fromString(stringifiedProperty);

		super.setup(context);
	}

	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<EvaluatedCandidateWritable<T>> arg1,
			Reducer<LongWritable, EvaluatedCandidateWritable<T>, LongWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		
		List<EvaluatedCandidate<T>> population = new ArrayList<EvaluatedCandidate<T>>();
		Iterator<EvaluatedCandidateWritable<T>> iterator = arg1.iterator();
		EvaluatedCandidateWritable<T> crtCandidate;
		
		while(iterator.hasNext()) {
			
			crtCandidate = iterator.next();
			population.add(new EvaluatedCandidate<T>(crtCandidate.getCandidate(), crtCandidate.getFitness()));
		}
		
		int parentsNumber = Math.max(population.size() / 2, 2);
		
		//Select the parents in order to generate the offsprings;
		List<T> parents = selectionStrategy.select(population, fitnessEvaluator.isNatural(), parentsNumber, rng);
		
		//Recombine/Mutate the parents to obtain the offsprings;
		List<T> offsprings = evolutionaryOperator.apply(parents, rng);
		
		//Evaluate the offprings
		List<EvaluatedCandidate<T>> evaluatedOffsprings = new ArrayList<EvaluatedCandidate<T>>(offsprings.size());
		for(int i = 0; i < offsprings.size(); i++) {
			
			evaluatedOffsprings.add(
					new EvaluatedCandidate<T>(
							offsprings.get(i), 
							fitnessEvaluator.getFitness(offsprings.get(i), offsprings)
					)
			);
		}
		
		population.addAll(evaluatedOffsprings);
		
		FSUtils fsUtils = new FSUtils(FileSystem.get(arg2.getConfiguration()));
		fsUtils.mkdir("output", false);
		//System.out.println("writing to " + "output/partition_" + arg2.getTaskAttemptID().getTaskID());
		fsUtils.writeToFile("output/partition_" + arg2.getTaskAttemptID().getTaskID(), StringUtils.toString(population));
	}
	
}
