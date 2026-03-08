import java.util.*;
public class ClusterScheduling{

// RESOURCE CLASS
class Resource {

    int machineId;
    int cpu;
    int memory;

    Resource(int machineId, int cpu, int memory) {
        this.machineId = machineId;
        this.cpu = cpu;
        this.memory = memory;
    }
}

// TASK CLASS
class Task {

    int cpuRequired;
    int memoryRequired;
    int duration;
    boolean isScheduled = false;

    Task(int cpu, int memory, int duration) {
        this.cpuRequired = cpu;
        this.memoryRequired = memory;
        this.duration = duration;
    }
}

// JOB CLASS
class Job {

    int jobId;
    List<Task> tasks;

    int schedulingAttempts = 0;
    long arrivalTime = System.currentTimeMillis();

    Job(int jobId, List<Task> tasks) {
        this.jobId = jobId;
        this.tasks = tasks;
    }
}

// STATE CLASS
class State {

    Map<Resource, Boolean> st1 = new HashMap<>();

    void addResource(Resource r) {
        st1.put(r, true);
    }

    boolean check(Resource r) {
        return st1.getOrDefault(r, false);
    }

    void setAvailability(Resource r, boolean status) {
        st1.put(r, status);
    }
}

// GLOBAL STATE
class GlobalState extends State {

    synchronized boolean tryScheduling(Task task, Resource machine) {

        boolean status = check(machine);

        if (status) {
            setAvailability(machine,false);
            return true;
        }

        return false;
    }

    synchronized void releaseResource(Resource machine){
        setAvailability(machine,true);
    }
}

// DELTA STATE
class DeltaState extends State {

    void syncCells(GlobalState gb) {

        st1.clear();

        for(Map.Entry<Resource, Boolean> entry : gb.st1.entrySet()){
            st1.put(entry.getKey(), entry.getValue());
        }
    }

    Resource checkwithDelta(Task task){

        for(Resource r : st1.keySet()){

            if(check(r)){
                return r;
            }
        }

        return null;
    }
}

// SCHEDULER CLASS
class Scheduler {

    private GlobalState globalState;
    private DeltaState deltaState;

    private boolean isBusy = false;

    private Queue<Task> taskQueue = new LinkedList<>();


    // simulation time
    long currentTime = 0;

    // machine finish time
    Map<Resource, Long> runningTasks = new HashMap<>();
    
    // METRICS
    int numSuccessfulTransactions = 0;
    int numFailedTransactions = 0;
    int numRetriedTransactions = 0;
    long thinkTime = 0;
    long usefulSchedulingTime = 0;
    long wastedSchedulingTime = 0;

    Scheduler(GlobalState globalState) {
        this.globalState = globalState;
        this.deltaState = new DeltaState();
        sync();
    }

    void addJob(Job job){

        for(Task t : job.tasks){
            taskQueue.add(t);
        }

        if(!isBusy){
            processNextTask();
        }
    }

    // release machines whose tasks finished
    void releaseFinishedTasks(){

        Iterator<Map.Entry<Resource, Long>> it =
                runningTasks.entrySet().iterator();

        while(it.hasNext()){

            Map.Entry<Resource, Long> entry = it.next();

            if(entry.getValue() <= currentTime){

                globalState.releaseResource(entry.getKey());

                it.remove();
            }
        }
    }

    private void processNextTask(){

        if(taskQueue.isEmpty()){
            isBusy = false;
            return;
        }

        isBusy = true;

        releaseFinishedTasks();

        Task task = taskQueue.poll();

        long startThink = System.nanoTime();

        Resource machine = deltaState.checkwithDelta(task);

        long endThink = System.nanoTime();

        long currentThinkTime = endThink - startThink;

        thinkTime += currentThinkTime;

        currentTime++;

        if(machine != null){
            commit(task, machine, currentThinkTime);
        }

        processNextTask();
    }

    private void commit(Task task, Resource machine, long currentThinkTime){

        boolean success = globalState.tryScheduling(task, machine);

        if(success){

            numSuccessfulTransactions++;
            usefulSchedulingTime += currentThinkTime;

            task.isScheduled = true;

            long finishTime = currentTime + task.duration;

            runningTasks.put(machine, finishTime);

        }
        else{

            numFailedTransactions++;
            numRetriedTransactions++;

            wastedSchedulingTime += currentThinkTime;

            taskQueue.add(task);
        }

        sync();
    }

    private void sync(){
        deltaState.syncCells(globalState);
    }
}

// WORKLOAD CLASS
class Workload {

    List<Job> jobs = new ArrayList<>();

    void generateJobs(int numJobs,
                      int tasksPerJob,
                      int cpuPerTask,
                      int memPerTask){

        Random rand = new Random();

        for(int i=1;i<=numJobs;i++){

            List<Task> tasks = new ArrayList<>();

            for(int j=0;j<tasksPerJob;j++){

                int duration = 2 + rand.nextInt(4);

                tasks.add(new Task(cpuPerTask,
                                   memPerTask,
                                   duration));
            }

            jobs.add(new Job(i,tasks));
        }
    }

    List<Job> getJobs(){
        return jobs;
    }
}


// MAIN SIMULATION
public static void main(String[] args){

    ClusterScheduling cs = new ClusterScheduling();

    GlobalState global = cs.new GlobalState();

    // create machines
    for(int i=0;i<5;i++){
        global.addResource(cs.new Resource(i,4,8));
    }

    // create schedulers
    int numSchedulers = 3;

    Scheduler[] schedulers = new Scheduler[numSchedulers];

    for(int i=0;i<numSchedulers;i++){
        schedulers[i] = cs.new Scheduler(global);
    }

    // generate workload
    Workload workload = cs.new Workload();

    workload.generateJobs(
            20, // jobs
            3,  // tasks per job
            2,  //cpu
            2   //mem
    );

    List<Job> jobs = workload.getJobs();
    int index = 0;

    for(Job job : jobs){

        schedulers[index].addJob(job);

        index = (index + 1) % numSchedulers;
    }


    for(int i=0;i<numSchedulers;i++){

        System.out.println("Scheduler "+i);
        System.out.println("Successful: "+schedulers[i].numSuccessfulTransactions);
        System.out.println("Failed: "+schedulers[i].numFailedTransactions);
        System.out.println("Retried: "+schedulers[i].numRetriedTransactions);
        System.out.println("ThinkTime: "+schedulers[i].thinkTime);
        System.out.println("UsefulTime: "+schedulers[i].usefulSchedulingTime);
        System.out.println("WastedTime: "+schedulers[i].wastedSchedulingTime);

        System.out.println();
    }

}

}