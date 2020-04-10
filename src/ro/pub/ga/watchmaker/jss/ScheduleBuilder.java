package ro.pub.ga.watchmaker.jss;

import java.util.ArrayList;
import java.util.List;

public class ScheduleBuilder {

	
	private List<List<Task>> jobs;
	
	
	public ScheduleBuilder(List<List<Task>> jobs) {
		super();
		this.jobs = jobs;
	}

	public long getScheduleMakespan(List<List<ScheduleItem>> schedulePrototype) {
		
		List<List<ScheduleItem>> schedule = buildSchedule(getCopyOfCandidate(schedulePrototype));
		long maxMakespan = schedule.get(0).get(schedule.get(0).size() - 1).getEnd();
		long crtMakespan = Long.MIN_VALUE;
		
		//System.out.println(ScheduleFactory.printSchedule(schedule));
		
		for(int i = 1; i < schedule.size(); i++) {
			
			crtMakespan = schedule.get(i).get(schedule.get(i).size() - 1).getEnd();
			if(crtMakespan > maxMakespan) {
				
				maxMakespan = crtMakespan;
			}
		}
		
		return maxMakespan;
	}
	
	public List<List<ScheduleItem>> buildSchedule(List<List<ScheduleItem>> schedule) {
		
		int selectedMachine = 0;
		long minMakespan = 0;
		int totalTasks = getTotalNumberOfTasks();
		int operationIndex;
		List<Integer> exclusions = new ArrayList<Integer>(schedule.size());
		List<List<ScheduleItem>> partialSchedule = new ArrayList<List<ScheduleItem>>(schedule.size());
		long[] makespans = new long[schedule.size()];
		
		for(int i = 0; i < schedule.size(); i++) {
			
			partialSchedule.add(new ArrayList<ScheduleItem>());
			makespans[i] = 0;
		}
		
		while(totalTasks > 0) {
		
			// Choose machine j such that Tj is minimum (in the case of a tie choose at random). 
			selectedMachine = getMachineWithMinimumMakespan(makespans, exclusions);
			minMakespan = makespans[selectedMachine];
					
			//  Compute set W i including all the candidate operations ready to work at time T~ (operation oi,j
			// is ready to work if there are no previous operations belonging to the same job i that have not
			// yet finished being processed).
			List<ScheduleItem> candidateOperations = computeCandidateOperations(schedule.get(selectedMachine), partialSchedule, minMakespan);
			if(candidateOperations.size() == 0) {
				
				exclusions.add(selectedMachine);
				
				continue;
			}
			
			// Compute the eligible operation o~j e Wj, with duration drj; trj = T i ~-drj will be the completion
			// time of orj if scheduled at time T i. 
			ScheduleItem eligibleOperation = new ScheduleItem(candidateOperations.get(0));
			long prevTaskEnd = eligibleOperation.getTaskNumber() > 0 
					? getScheduledTaskEndTime(partialSchedule, eligibleOperation.getJobNumber(), eligibleOperation.getTaskNumber() - 1) 
					: 0;
					
			eligibleOperation.setStart(Math.max(minMakespan, prevTaskEnd) + 1);
			eligibleOperation.setEnd(eligibleOperation.getStart() + jobs.get(eligibleOperation.getJobNumber()).get(eligibleOperation.getTaskNumber()).getExecutionTime());
		    
			// Compute set Prj including all the operations that are positioned before orj in the original sequence
			// and that will be ready to start before the instant t~j.
			//IF Prj = ~ THEN schedule orj and set To~ d = T~, Tj = t~j
			//ELSE set machine j to an idle state. 
			operationIndex = schedule.get(selectedMachine).indexOf(candidateOperations.get(0));
				
			makespans[selectedMachine] = eligibleOperation.getEnd();
			schedule.get(selectedMachine).remove(operationIndex);
			partialSchedule.get(selectedMachine).add(eligibleOperation);
			exclusions.clear();
			totalTasks--;
		}
		
		return partialSchedule;
	}

	private boolean checkPrecedenceConstraint(ScheduleItem task, List<List<ScheduleItem>> schedule, long startTime) {
		
		//If it is the first task of the job then no precedence check is necessary
		if(task.getTaskNumber() == 0) {
			
			return true;
		}
			
		long prevEndTime = getScheduledTaskEndTime(schedule, task.getJobNumber(), task.getTaskNumber() - 1);		
		return prevEndTime >= 0 && prevEndTime <= startTime;
	}
	
	private boolean checkSimplePrecedenceConstraint(ScheduleItem task, List<List<ScheduleItem>> schedule) {
		
		//If it is the first task of the job then no precedence check is necessary
		if(task.getTaskNumber() == 0) {
			
			return true;
		}
			
		long prevEndTime = getScheduledTaskEndTime(schedule, task.getJobNumber(), task.getTaskNumber() - 1);
		
		return prevEndTime >= 0;
	}
	
	private long getScheduledTaskEndTime(List<List<ScheduleItem>> schedule, int jobNumber, int taskNumber) {
		
		Task task = jobs.get(jobNumber).get(taskNumber);
		
		//Find the scheduling information for the job on the necessary machine; 
		List<ScheduleItem> machineSchedule = schedule.get(task.getMachineNumber());
		for(int i = 0; i < machineSchedule.size(); i++) {
			
			if(machineSchedule.get(i).getJobNumber() == jobNumber
					&& machineSchedule.get(i).getTaskNumber() == taskNumber) {
				
				return machineSchedule.get(i).getEnd();
			}
		}
		
		return -1;
	}
	
	private int getMachineWithMinimumMakespan(long[] makespans, List<Integer> exclusions) {
		
		long minMakespan = Long.MAX_VALUE;
		int machine = -1;
		
		for(int i = 0; i < makespans.length; i++) {
			
			if(exclusions.contains(i)) {
				
				continue;
			}
			
			if(makespans[i] == 0) {
				
				return i;
			}
			
			if(minMakespan > makespans[i]) {
				
				minMakespan = makespans[i];
				machine = i;
			}
		}
		
		return machine;
	}
	
	private List<ScheduleItem> computeCandidateOperations(List<ScheduleItem> unscheduledTasks, List<List<ScheduleItem>> partialSchedule, long startTime) {
		
		List<ScheduleItem> candidateOperations = new ArrayList<ScheduleItem>();
		for(int i = 0; i < unscheduledTasks.size(); i++) {
			
			if(checkSimplePrecedenceConstraint(unscheduledTasks.get(i), partialSchedule)) {
				
				candidateOperations.add(unscheduledTasks.get(i));
			}
		}
		
		return candidateOperations;
	}
	
	private int getTotalNumberOfTasks() {
		
		int total = 0;
		
		for(List<Task> job: jobs) {
			
			total += job.size();
		}
		
		return total;
	}
	
	public List<List<ScheduleItem>> getCopyOfCandidate(List<List<ScheduleItem>> candidate) {
		
		List<List<ScheduleItem>> copy = new ArrayList<List<ScheduleItem>>(candidate.size());
		for(int i = 0; i < candidate.size(); i++) {
			
			copy.add(new ArrayList<ScheduleItem>(candidate.get(i)));
		}
		
		return copy;
	}
}
