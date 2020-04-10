package ro.pub.ga.watchmaker.hybrid.core;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.common.StringUtils;
import org.uncommons.watchmaker.framework.FitnessEvaluator;

import ro.pub.ga.watchmaker.core.Constants;
import ro.pub.ga.watchmaker.example.clustering.CenterListEvaluator;
import ro.pub.ga.watchmaker.hybrid.core.DistributedHybridEvolutionEngine.ApplicationType;
import ro.pub.ga.watchmaker.utils.AssignedCandidateWritable;
import ro.pub.ga.watchmaker.utils.EvaluatedCandidateWritable;

import com.google.common.base.Preconditions;

public class MasterSlaveIslandsMapper<T> extends Mapper<LongWritable,Text,LongWritable, EvaluatedCandidateWritable<T>> {

	private FitnessEvaluator<Object> evaluator;
	private Integer numIndividuals;
	private Configuration conf;
	private ApplicationType applicationType;
	
	@Override
	public void setup(Context context) throws IOException, InterruptedException {

		conf = context.getConfiguration();
		String evlstr = conf.get(Constants.FITNESS_EVALUATOR_PROPERTY);
		
		Preconditions.checkArgument(evlstr != null, "'FITNESS_EVALUATOR_PROPERTY' job parameter not found");

		evaluator = StringUtils.fromString(evlstr);
		numIndividuals = 0;
		applicationType = StringUtils.fromString(conf.get(Constants.APPLICATION_TYPE));
		super.setup(context);
	}

	@Override
	public void map(LongWritable key,
			Text value,
			Context context) throws IOException, InterruptedException {

		//iau candidatul
		AssignedCandidateWritable<T> assignedCandidate = StringUtils.fromString(value.toString());
		// preiau indexul subpopulatiei
		Long subpopulationIndex = (Long)assignedCandidate.getAssignedSubpopulation();
		//evaluez
		double fitness = 0;
		if (applicationType == ApplicationType.Clustering) {
			System.out.println("[Mapper] Clustering app => modify the candidate while getting fitness");
			// Optimization for Clustering apps - we modify the candidate when evaluating
			CenterListEvaluator centerListEvaluator = (CenterListEvaluator)((Object)evaluator);
			fitness = centerListEvaluator.getFitnessAndUpdateCandidate((List<Float>)assignedCandidate.getCandidate(), null);
		} else {
			fitness = evaluator.getFitness(assignedCandidate.getCandidate(), null);
		}
		//scriu pt reducer
		context.write(new LongWritable(subpopulationIndex), new EvaluatedCandidateWritable<T>(assignedCandidate.getCandidate(), fitness));
		numIndividuals++;
		System.out.println("candidate from subpop " + subpopulationIndex + " evaluated with fitness " + fitness + "\n");
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, LongWritable, EvaluatedCandidateWritable<T>>.Context context)
	throws IOException, InterruptedException {
		super.cleanup(context);
		java.net.InetAddress localMachine = java.net.InetAddress.getLocalHost();
		System.out.println("[Mapper:" + localMachine.getHostName() +"]Number of processed individuals: " + numIndividuals);
	}
}
