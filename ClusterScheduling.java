package clusterScheduling;
import java.util.*;
public class ClusterScheduling{
 class Resource {

    int machineId;
    int cpu;
    int memory;

    public Resource(int machineId, int cpu, int memory) {
        this.machineId = machineId;
        this.cpu = cpu;
        this.memory = memory;
    }
}

//TASK CLASS
class Task {

    int cpuRequired;
    int memoryRequired;
    boolean isScheduled = false;

    public Task(int cpu, int memory) {
        this.cpuRequired = cpu;
        this.memoryRequired = memory;
    }
}

//JOB CLASS
class Job {

    int jobId;
    List<Task> tasks;

    public Job(int jobId, List<Task> tasks) {
        this.jobId = jobId;
        this.tasks = tasks;
    }
}

//STATE CLASS
class State {

    Map<Resource, Boolean> st1 = new HashMap<>();

    public void addResource(Resource r) {
        st1.put(r, true);  
// true = initially available
    }

    public boolean check(Resource r) {
        return st1.getOrDefault(r, false);
    }

    public void setAvailability(Resource r, boolean status) {
        st1.put(r, status);
    }
}

//GLOBAL STATE
class GlobalState extends State {
    public boolean tryScheduling(Job job, Resource machine) {

        boolean status = check(machine);

        if (status == true) {
            setAvailability(machine, false); 
             // mark busy
            return true;
        }

        return false;
    }
}

//DELTA STATE
class DeltaState extends State {
    // Sync private copy from global
    public void syncCells(GlobalState gb) {

        st1.clear();
    //fresh copy so we are clearing the current states

        for (Map.Entry<Resource, Boolean> entry : gb.st1.entrySet()) {
            st1.put(entry.getKey(), entry.getValue());
        }
    }
    public Resource checkwithDelta(Job job) {

        for (Resource r : st1.keySet()) {

            if (check(r) == true) {
                return r;  // return first available machine
            }
        }
        return null;
    }
}
//SCHEDULER CLASS
class Scheduler {

    private GlobalState globalState;
    private DeltaState deltaState;

    private boolean isBusy = false;

    private Queue<Job> jobQueue = new LinkedList<>();

    public Scheduler(GlobalState globalState) {
        this.globalState = globalState;
        this.deltaState = new DeltaState();
        sync();
    }

    public void addJob(Job job) {
        jobQueue.add(job);

        if (!isBusy) {
            processNextJob();
        }
    }

    private void processNextJob() {

        if (jobQueue.isEmpty()) {
            isBusy = false;
            return;
        }

        isBusy = true;

        Job job = jobQueue.poll();

        Resource machine = deltaState.checkwithDelta(job);

        if (machine != null) {
            commit(job, machine);
        }

        processNextJob();
    }

    private void commit(Job job, Resource machine) {

        boolean success = globalState.tryScheduling(job, machine);

        if (!success) {
            jobQueue.add(job);  // retry later
        }

        sync();
    }

    private void sync() {
        deltaState.syncCells(globalState);
    }

    public boolean isBusy() {
        return isBusy;
    }
}
}