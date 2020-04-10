package ro.pub.ga.watchmaker.hybrid.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.EvaluatedCandidate;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

import ro.pub.ga.watchmaker.core.Constants;
import ro.pub.ga.watchmaker.utils.FSUtils;
import ro.pub.ga.watchmaker.utils.PopulationWritable;

public class MicroMacroIslandsReducer<T> extends Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text>  {

	private Integer macropopulationIndex;
	private FitnessEvaluator<T> fitnessEvaluator;
	private Boolean doMigration;
	private String outpath;
	private FSUtils fsUtils;
	
	@Override
	protected void setup(
			Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		
		fitnessEvaluator = StringUtils.fromString(conf.get(Constants.FITNESS_EVALUATOR_PROPERTY));
		doMigration = conf.getBoolean(Constants.MIGRATION_PROPERTY, false);
		outpath = conf.get("mapreduce.output.fileoutputformat.outputdir");
		fsUtils = new FSUtils(FileSystem.get(conf));
		
		super.setup(context);
	}
	
	@Override
	protected void reduce(
			LongWritable arg0,
			Iterable<PopulationWritable<EvaluatedCandidate<T>>> arg1,
			Reducer<LongWritable, PopulationWritable<EvaluatedCandidate<T>>, LongWritable, Text>.Context arg2)
			throws IOException, InterruptedException {
		
		macropopulationIndex = (int)arg0.get();
		
		List<PopulationWritable<EvaluatedCandidate<T>>> populations = new ArrayList<PopulationWritable<EvaluatedCandidate<T>>>();
		Iterator<PopulationWritable<EvaluatedCandidate<T>>> iterator = arg1.iterator();
		
		PopulationWritable<EvaluatedCandidate<T>> crtPopulation;
		PopulationWritable<EvaluatedCandidate<T>> externalMigrators = null;
		
		while(iterator.hasNext()) {
			crtPopulation = iterator.next();
			if ((Integer)crtPopulation.getDataValue(Constants.MICROPOPULATION_INDEX) == (int)Constants.MIGRATOR_MICROPOPULATION_INDEX) {
				externalMigrators = crtPopulation;
			} else {
				populations.add(new PopulationWritable<EvaluatedCandidate<T>>(crtPopulation.getPopulation(), crtPopulation.getData()));
			}
		}
		
		performMigration(populations, externalMigrators);
		
		java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
		System.out.println("[Oana][Reducer]" +localMachine.getHostName() + " population " + 
				" macro " + macropopulationIndex + " nr of micropop " + populations.size());
		
		PopulationWritable<EvaluatedCandidate<T>> evolvedPopulation = new PopulationWritable<EvaluatedCandidate<T>>();
		for(int i = 0; i < populations.size(); i++) {
			evolvedPopulation.addToPopulation(populations.get(i).getPopulation());
		}
		
		//Sort the individuals in the evolved population according to the fitness values
		if (fitnessEvaluator.isNatural()) {
			Collections.sort(evolvedPopulation.getPopulation(), Collections.reverseOrder());
		} else {
			Collections.sort(evolvedPopulation.getPopulation());
		}
		
		// Write population to files
		fsUtils.mkdir(outpath, false);
		EvaluatedCandidate<T> bestCandidate = evolvedPopulation.getPopulation().get(0);
		writeBestCandidateToFile(new EvaluatedCandidate<T>(bestCandidate.getCandidate(), bestCandidate.getFitness()));
		writeEvolvedMacropopulationToFile(evolvedPopulation.getPopulation());
		writeMicropopulationsToFile(populations);
	}
	
	public void writeMicropopulationsToFile(List<PopulationWritable<EvaluatedCandidate<T>>> populations) {
		 int populationIndex;
		 for(PopulationWritable<EvaluatedCandidate<T>> crtPopulation: populations) {
			populationIndex = (Integer)crtPopulation.getDataValue(Constants.MICROPOPULATION_INDEX);
			crtPopulation.getData().clear();
			crtPopulation.putDataValue(Constants.MICROPOPULATION_INDEX, (int)populationIndex);
			crtPopulation.putDataValue(Constants.MACROPOPULATION_INDEX, (int)macropopulationIndex);
			fsUtils.writeToFile(
					outpath + "/subpopulation_" + macropopulationIndex + "_" + populationIndex, 
					StringUtils.toString(
							new PopulationWritable<T>(
									crtPopulation.getPopulation(), 
									crtPopulation.getData(), 
									true)
					)
			);
		 }
	 }
	
	public void writeBestCandidateToFile(EvaluatedCandidate<T> bestCandidate) {
		fsUtils.writeToFile(outpath + "/best_candidate_" + macropopulationIndex, StringUtils.toString(bestCandidate));
	}
	
	public void writeEvolvedMacropopulationToFile(List<EvaluatedCandidate<T>> population) {
		fsUtils.writeToFile(outpath + "/evolved_population_" + macropopulationIndex, StringUtils.toString(population));
	}
	
	private void performMigration(List<PopulationWritable<EvaluatedCandidate<T>>> populations,
			PopulationWritable<EvaluatedCandidate<T>> migrators) {
		if(doMigration == false) {
			return;
		}
		StringBuilder sb = new StringBuilder();
		PopulationWritable<EvaluatedCandidate<T>> crtPopulation = null;
		int nextPopulationIndex;
		sb.append("[" + macropopulationIndex + "]Local migrations: ");
		// Local migrations (inside the macroisland)
		for(int i = 0; i < populations.size(); i++) {
			crtPopulation = populations.get(i);
			nextPopulationIndex = (i + 1) % populations.size();
			List<EvaluatedCandidate<T>> localMigrators = 
				(List<EvaluatedCandidate<T>>)crtPopulation.getDataValue(Constants.INDIVIDUALS_TO_MIGRATE);
			if (localMigrators != null) {
				populations.get(nextPopulationIndex).addToPopulation(localMigrators);
				sb.append(" from " + i + " to " + nextPopulationIndex);
			}
		}
		
		if (migrators != null) {
			sb.append("\nExternal migrations nr: " + migrators.getPopulation().size());
			// External migrations (migrators from outside the macroisland)
			for (int i = 0; i < migrators.getPopulation().size(); i++) {
				int populationIndex = i > populations.size() - 1 ? i % populations.size() : i;   
				populations.get(populationIndex).getPopulation().add(migrators.getPopulation().get(i));
			}
		}
		System.out.println(sb.toString());
	}
}
