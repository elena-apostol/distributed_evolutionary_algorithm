package ro.pub.ga.watchmaker.jss;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.uncommons.watchmaker.framework.CandidateFactory;
import org.uncommons.watchmaker.framework.factories.ListPermutationFactory;

public class ScheduleFactory implements CandidateFactory<List<List<ScheduleItem>>>{
	
	private int numberOfJobs;
	private int numberOfMachines;
	private List<List<Task>> jobs;
	private List<List<ScheduleItem>> seedSchedule;
	private List<ListPermutationFactory<ScheduleItem>> listPermutation;
	
	public ScheduleFactory(List<List<Task>> jobs, Random rng) {
		
		this.jobs = jobs;
		generateSeedSchedule();
		this.listPermutation = new ArrayList<ListPermutationFactory<ScheduleItem>>();
		for(int i = 0; i < seedSchedule.size(); i++) {
			
			listPermutation.add(new ListPermutationFactory<ScheduleItem>(seedSchedule.get(i)));
		}
	}
	
	public ScheduleFactory(String inputFile, Random rng) {
		
		try {
			
			parseInput(inputFile);
			generateSeedSchedule();
			this.listPermutation = new ArrayList<ListPermutationFactory<ScheduleItem>>();
			for(int i = 0; i < seedSchedule.size(); i++) {
				
				listPermutation.add(new ListPermutationFactory<ScheduleItem>(seedSchedule.get(i)));
			}
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public List<List<List<ScheduleItem>>> generateInitialPopulation(
			int populationSize, Random rng) {
		
		List<List<List<ScheduleItem>>> population = new ArrayList<List<List<ScheduleItem>>>(populationSize);
		for(int i = 0; i < populationSize; i++) {
			
			population.add(generateRandomCandidate(rng));
		}
		
		return population;
	}

	@Override
	public List<List<List<ScheduleItem>>> generateInitialPopulation(
			int populationSize,
			Collection<List<List<ScheduleItem>>> seedCandidates, Random rng) {
		
		List<List<List<ScheduleItem>>> population = new ArrayList<List<List<ScheduleItem>>>(populationSize);
		if(seedCandidates != null) {
			
			population.addAll(seedCandidates);
		}
		
		for(int i = population.size(); i < populationSize; i++) {
			
			population.add(generateRandomCandidate(rng));
		}
		
		return population;
	}

	@Override
	public List<List<ScheduleItem>> generateRandomCandidate(Random rng) {
		
		List<List<ScheduleItem>> individual = new ArrayList<List<ScheduleItem>>(seedSchedule.size());
		for(int i = 0; i < seedSchedule.size(); i++) {
			
			individual.add(listPermutation.get(i).generateRandomCandidate(rng));
		}
		
		return individual;
	}
	
	private void parseInput(String inputFile) throws Exception {
		
		Path pt=new Path(inputFile);
	    FileSystem fs = FileSystem.get(new Configuration());
	    BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
	    
	    String line = null;
	    String[] tokens;
	    
	    line = reader.readLine();
	    tokens = line.trim().split(" ");
	    numberOfJobs = Integer.parseInt(tokens[0]);
	    numberOfMachines = Integer.parseInt(tokens[1]);
	    
	    jobs = new ArrayList<List<Task>>(numberOfJobs);
	    for(int i = 0; i < numberOfJobs; i++) {
	    	
	    	jobs.add(new ArrayList<Task>());
	    	
	    	line = reader.readLine();
	    	tokens = line.trim().split(" ");
	    	for(int j = 0; j < (tokens.length / 2); j++) {
	    		
	    		jobs.get(i).add(new Task(Integer.parseInt(tokens[2 * j]), Long.parseLong(tokens[2 * j + 1])));
	    	}
	    }
	    
	    reader.close();
	}
	
	private void generateSeedSchedule() {
		
		seedSchedule = new ArrayList<List<ScheduleItem>>(numberOfMachines);
		for(int i = 0; i < numberOfMachines; i++) {
			
			seedSchedule.add(new ArrayList<ScheduleItem>());
		}
		
		int jobNumber = 0;
		int taskNumber = 0;
		for(List<Task> job: jobs) {
			
			taskNumber = 0;
			for(Task task: job) {
				
				seedSchedule.get(task.getMachineNumber()).add(new ScheduleItem(taskNumber++, jobNumber, task.getMachineNumber()));
			}
			jobNumber++;
		}
	}


	public static String printJobs(List<List<Task>> jobs) {
		
		
		StringBuilder builder = new StringBuilder("");
		for(List<Task> job: jobs) {
			
			builder.append(job.toString() + "\n");
		}
		
		return builder.toString();
	}
	
	public static String printSchedule(List<List<ScheduleItem>> schedule) {
		
		StringBuilder builder = new StringBuilder("");
		for(List<ScheduleItem> item: schedule) {
			
			builder.append(item.toString() + "\n");
		}
		
		return builder.toString();
	}

	public int getNumberOfJobs() {
		return numberOfJobs;
	}

	public void setNumberOfJobs(int numberOfJobs) {
		this.numberOfJobs = numberOfJobs;
	}

	public int getNumberOfMachines() {
		return numberOfMachines;
	}

	public void setNumberOfMachines(int numberOfMachines) {
		this.numberOfMachines = numberOfMachines;
	}

	public List<List<Task>> getJobs() {
		return jobs;
	}

	public void setJobs(List<List<Task>> jobs) {
		this.jobs = jobs;
	}

	public List<List<ScheduleItem>> getSeedSchedule() {
		return seedSchedule;
	}

	public void setSeedSchedule(List<List<ScheduleItem>> seedSchedule) {
		this.seedSchedule = seedSchedule;
	}

	public List<ListPermutationFactory<ScheduleItem>> getListPermutation() {
		return listPermutation;
	}

	public void setListPermutation(
			List<ListPermutationFactory<ScheduleItem>> listPermutation) {
		this.listPermutation = listPermutation;
	}
}
