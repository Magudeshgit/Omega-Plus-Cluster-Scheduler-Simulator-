package mesos_stimulator;

import java.util.*;

/* EVENT */

class SimEvent implements Comparable<SimEvent>{

    double time;
    Runnable action;

    SimEvent(double t,Runnable a){
        time=t;
        action=a;
    }

    public int compareTo(SimEvent e){
        return Double.compare(time,e.time);
    }
}


/* LOGGER */

class Logger{

    static void header(){

        System.out.println("---------------------------------------------------------------------------------------------------------------");

        System.out.printf("%-10s %-9s %-4s %-4s %-10s %-7s %-4s %-6s %-10s %-9s %-9s %-8s %-9s %-9s\n",
                "Event","Scheduler","Job","Task","Workload","Machine","CPU","MEM",
                "Submit(ms)","Queue(ms)","Think(ms)","Exec(ms)","Start(ms)","Finish(ms)");

        System.out.println("---------------------------------------------------------------------------------------------------------------");
    }

    static void entry(double time,String sched,Job job){

        System.out.printf("%-10s %-9s %-4s %-4s %-10s %-7s %-4s %-6s %-10.0f %-9s %-9s %-8s %-9s %-9s\n",
                "ENTRY",sched,"J"+job.id,"-",job.workload,"-","-","-",
                job.submit,"-","-","-","-","-");
    }

    static void queued(double time,String sched,Job job,double think){

        System.out.printf("%-10s %-9s %-4s %-4s %-10s %-7s %-4s %-6s %-10.0f %-9s %-9.0f %-8s %-9s %-9s\n",
                "QUEUED",sched,"J"+job.id,"-",job.workload,"-","-","-",
                job.submit,"-",think,"-","-","-");
    }

    static void allocated(double start,String sched,Job job,int taskId,int machine,
                      double cpu,double mem,double submit,double think,double exec){

        double finish=start+exec;
        double queue=start-submit-think;
        if(queue<0) queue=0;

        System.out.printf("%-10s %-9s %-4s %-4s %-10s %-7s %-4.0f %-6.0f %-10.0f %-9.0f %-9.0f %-8.0f %-9.0f %-9.0f\n",
                "ALLOCATED",sched,"J"+job.id,"T"+taskId,job.workload,"M-"+machine,
                cpu,mem,submit,queue,think,exec,start,finish);
    }

    static void finished(double time,String sched,Job job,int taskId,int machine,
                       double cpu,double mem,double submit,double exec,double start){

        double queue=start-submit;

        System.out.printf("%-10s %-9s %-4s %-4s %-10s %-7s %-4.0f %-6.0f %-10.0f %-9.0f %-9s %-8.0f %-9.0f %-9.0f\n",
                "FINISHED",sched,"J"+job.id,"T"+taskId,job.workload,"M-"+machine,
                cpu,mem,submit,queue,"-",exec,start,time);
    }
}


/* METRICS */

class SchedulerMetrics{

    int successful;
    int failed;
    int retried;

    double thinkTime;
    double usefulTime;
    double wastedTime;
}


/* CELL */

class CellState{

    int machines;
    double cpuPerMachine;
    double memPerMachine;

    double usedCpu=0;
    double usedMem=0;

    Map<String,Double> schedCpu=new HashMap<>();
    Map<String,Double> schedMem=new HashMap<>();

    CellState(int m,double cpu,double mem){
        machines=m;
        cpuPerMachine=cpu;
        memPerMachine=mem;
    }

    double totalCpu(){ return machines*cpuPerMachine; }
    double totalMem(){ return machines*memPerMachine; }

    double availableCpu(){ return totalCpu()-usedCpu; }
    double availableMem(){ return totalMem()-usedMem; }

    boolean allocate(String sched,double cpu,double mem){

        if(availableCpu()>=cpu && availableMem()>=mem){

            usedCpu+=cpu;
            usedMem+=mem;

            schedCpu.merge(sched,cpu,Double::sum);
            schedMem.merge(sched,mem,Double::sum);

            return true;
        }

        return false;
    }

    void free(String sched,double cpu,double mem){

        usedCpu-=cpu;
        usedMem-=mem;

        schedCpu.merge(sched,-cpu,Double::sum);
        schedMem.merge(sched,-mem,Double::sum);
    }

    int findMachine(){
        return (int)(Math.random()*machines);
    }

    CellState copy(){

        CellState c=new CellState(machines,cpuPerMachine,memPerMachine);

        c.usedCpu=usedCpu;
        c.usedMem=usedMem;

        c.schedCpu=new HashMap<>(schedCpu);
        c.schedMem=new HashMap<>(schedMem);

        return c;
    }
}


/* JOB */

class Job{

    static int idGen=0;

    int id;

    double cpu;
    double mem;
    double duration;
    double submit;

    int totalTasks;
    int unscheduledTasks;

    int retryCount=0;
    int nextTaskId=1;

    String workload;

    Job(double c,double m,double d,double s,int tasks,String w){

        id=idGen++;

        cpu=c;
        mem=m;
        duration=d;
        submit=s;

        totalTasks=tasks;
        unscheduledTasks=tasks;

        workload=w;
    }
}


/* CLAIM */

class Claim{

    Scheduler scheduler;
    Job job;

    double cpu;
    double mem;
    double duration;
    double think;

    Claim(Scheduler s,Job j,double c,double m,double d,double t){

        scheduler=s;
        job=j;
        cpu=c;
        mem=m;
        duration=d;
        think=t;
    }
}


/* OFFER */

class Offer{

    long id;
    Scheduler scheduler;
    CellState snapshot;

    Offer(long id,Scheduler s,CellState c){

        this.id=id;
        scheduler=s;
        snapshot=c.copy();
    }
}


/* SCHEDULER */

class Scheduler{

    String name;

    Queue<Job> queue=new LinkedList<>();

    Simulator sim;

    SchedulerMetrics metrics=new SchedulerMetrics();

    int maxRetries=8;

    Map<String,Double> constantThinkTimes = Map.of(
            "web",300.0,
            "ml",500.0,
            "analytics",400.0
    );

    Map<String,Double> perTaskThinkTimes = Map.of(
            "web",100.0,
            "ml",200.0,
            "analytics",150.0
    );

    Scheduler(String name){
        this.name=name;
    }

    void addJob(Job j){

        queue.add(j);

        Logger.entry(sim.time,name,j);

        sim.allocator.requestOffer(this);
    }

    double thinkTime(Job j){

        double c=constantThinkTimes.getOrDefault(j.workload,300.0);
        double p=perTaskThinkTimes.getOrDefault(j.workload,100.0);

        return c + p*j.unscheduledTasks;
    }

    void receiveOffer(Offer offer){

        if(queue.isEmpty()) return;

        Job job=queue.peek();

        double think=thinkTime(job);

        metrics.thinkTime+=think;

        sim.afterDelay(think,()->{

            List<Claim> claims=new ArrayList<>();

            while(job.unscheduledTasks>0 &&
                    offer.snapshot.availableCpu()>=job.cpu &&
                    offer.snapshot.availableMem()>=job.mem){

                claims.add(new Claim(this,job,job.cpu,job.mem,job.duration,think));

                offer.snapshot.usedCpu+=job.cpu;
                offer.snapshot.usedMem+=job.mem;

                job.unscheduledTasks--;
            }

            if(claims.isEmpty()){

                metrics.wastedTime+=think;

                job.retryCount++;

                if(job.retryCount>maxRetries){

                    metrics.failed++;
                    queue.poll();

                }else{

                    metrics.retried++;
                    Logger.queued(sim.time,name,job,think);
                    sim.allocator.requestOffer(this);
                }

            }else{

                metrics.successful++;
                metrics.usefulTime+=claims.size()*job.duration;

                if(job.unscheduledTasks==0)
                    queue.poll();
            }

            sim.allocator.commitClaims(claims);
        });
    }
}


/* ALLOCATOR */

class Allocator{

    Simulator sim;

    Set<Scheduler> requesting=new HashSet<>();

    double allocatorThinkTime=100;
    double offerBatchInterval=200;

    long nextOfferId=0;

    void requestOffer(Scheduler s){

        requesting.add(s);

        scheduleNextOffer();
    }

    void scheduleNextOffer(){

        sim.afterDelay(offerBatchInterval,this::buildOffer);
    }

    double dominantShare(Scheduler s){

        double cpuShare =
                sim.cell.schedCpu.getOrDefault(s.name,0.0)
                        /sim.cell.totalCpu();

        double memShare =
                sim.cell.schedMem.getOrDefault(s.name,0.0)
                        /sim.cell.totalMem();

        return Math.max(cpuShare,memShare);
    }

    void buildOffer(){

        if(requesting.isEmpty()) return;

        List<Scheduler> list=new ArrayList<>(requesting);

        list.sort(Comparator.comparingDouble(this::dominantShare));

        Scheduler chosen=list.get(0);

        requesting.remove(chosen);

        Offer offer=new Offer(nextOfferId++,chosen,sim.cell);

        sim.afterDelay(allocatorThinkTime,()->{
            chosen.receiveOffer(offer);
        });
    }

    void commitClaims(List<Claim> claims){

        for(Claim claim:claims){

            if(sim.cell.allocate(claim.scheduler.name,claim.cpu,claim.mem)){

                int machine=sim.cell.findMachine();

                int taskId = claim.job.nextTaskId++;

                Logger.allocated(sim.time,claim.scheduler.name,claim.job,
                        taskId,machine,claim.cpu,claim.mem,
                        claim.job.submit,claim.think,claim.duration);

                double startTime=sim.time;

                sim.afterDelay(claim.duration,()->{

                    sim.cell.free(claim.scheduler.name,claim.cpu,claim.mem);

                    Logger.finished(sim.time,claim.scheduler.name,claim.job,
                            taskId,machine,claim.cpu,claim.mem,
                            claim.job.submit,claim.duration,startTime);

                    scheduleNextOffer();
                });
            }
        }
    }
}


/* SIMULATOR */

class Simulator{

    double time=0;

    PriorityQueue<SimEvent> events=new PriorityQueue<>();

    CellState cell;

    Allocator allocator=new Allocator();

    Map<String,Scheduler> schedulers=new HashMap<>();

    Simulator(CellState cell){

        this.cell=cell;

        allocator.sim=this;
    }

    void afterDelay(double d,Runnable a){
        events.add(new SimEvent(time+d,a));
    }

    void run(){

        while(!events.isEmpty()){

            SimEvent e=events.poll();

            time=e.time;

            e.action.run();
        }

        printMetrics();
    }

    void printMetrics(){

        System.out.println();
        System.out.println("================ Scheduler Metrics ================");

        for(Scheduler s : schedulers.values()){

            SchedulerMetrics m = s.metrics;

            System.out.println("Scheduler: "+s.name);
            System.out.println("Successful: "+m.successful);
            System.out.println("Failed: "+m.failed);
            System.out.println("Retried: "+m.retried);
            System.out.println("ThinkTime(ms): "+m.thinkTime);
            System.out.println("UsefulTime(ms): "+m.usefulTime);
            System.out.println("WastedTime(ms): "+m.wastedTime);
            System.out.println("--------------------------------------------");
        }
    }
}


/* MAIN */

public class Main{

    public static void main(String[] args){

        Logger.header();

        CellState cell=new CellState(50,16,64000);

        // PRELOAD CLUSTER (simulate already busy machines)
        cell.usedCpu = cell.totalCpu() * 0.6;
        cell.usedMem = cell.totalMem() * 0.5;

        Simulator sim=new Simulator(cell);

        //  CREATE 9 SCHEDULERS
        Scheduler sA=new Scheduler("S-A");
        Scheduler sB=new Scheduler("S-B");
        Scheduler sC=new Scheduler("S-C");
        Scheduler sD=new Scheduler("S-D");
        Scheduler sE=new Scheduler("S-E");
        Scheduler sF=new Scheduler("S-F");
        Scheduler sG=new Scheduler("S-G");
        Scheduler sH=new Scheduler("S-H");
        Scheduler sI=new Scheduler("S-I");

        // Attach simulator
        Scheduler[] schedulers = {sA,sB,sC,sD,sE,sF,sG,sH,sI};

        for(Scheduler s : schedulers){
            s.sim = sim;
            sim.schedulers.put(s.name, s);
        }

        Random r=new Random();

        String[] workloads={"web","ml","analytics"};

        int TOTAL_JOBS = 600;

        //  DISTRIBUTE JOBS ACROSS 9 SCHEDULERS
        for(int i=0;i<TOTAL_JOBS;i++){

            String w=workloads[r.nextInt(workloads.length)];

            Job j=new Job(
                    r.nextInt(4)+1,                  // CPU
                    256*(r.nextInt(8)+1),            // MEM
                    r.nextInt(4000)+2000,            // duration
                    r.nextDouble()*20000,            // submit
                    r.nextInt(80)+20,                // tasks
                    w
            );

            //  ROUND ROBIN ACROSS 9 SCHEDULERS
            Scheduler target = schedulers[i % schedulers.length];

            sim.afterDelay(j.submit,()->target.addJob(j));
        }

        sim.run();
    }
}
