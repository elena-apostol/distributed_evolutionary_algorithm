package ro.pub.ga.watchmaker.jss;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.uncommons.watchmaker.framework.FitnessEvaluator;

public class ScheduleEvaluator implements FitnessEvaluator<List<List<ScheduleItem>>> {
	
	private int machinesNumber;
	private int jobsNumber;
	private Random rng;
	private List<List<Task>> jobs;
	
	public ScheduleEvaluator(int machinesNumber, int jobsNumber, Random rng,
			List<List<Task>> jobs) {

		this.machinesNumber = machinesNumber;
		this.jobsNumber = jobsNumber;
		this.rng = rng;
		this.jobs = jobs;
	}

	@Override
	public double getFitness(List<List<ScheduleItem>> candidate,
			List<? extends List<List<ScheduleItem>>> population) {
		
		ScheduleBuilder scheduleBuilder = new ScheduleBuilder(jobs);
		long makespan = scheduleBuilder.getScheduleMakespan(candidate);
		
		
		return makespan;
	}
	

	@Override
	public boolean isNatural() {
		
		return false;
	}
}
