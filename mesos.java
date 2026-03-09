package mesos_stimulator;

import java.util.*;

// EVENT

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


// LOGGER

class EventLogger {

    static void header(){

        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");

        System.out.printf(
        "%-7s %-9s %-8s %-4s %-6s %-7s %-6s %-6s %-7s %-8s %-8s %-8s %-8s %-7s\n",
        "Start","Scheduler","Machine","CPU","MEM","Submit","Think","Exec","Finish",
        "AvailCPU","AvailMEM","UsedCPU","UsedMEM","Event");

        System.out.println("--------------------------------------------------------------------------------------------------------------------------------");
    }

    static void start(double time,String sched,int machine,double cpu,double mem,
                      double submit,double think,double exec,
                      double availCpu,double availMem,double usedCpu,double usedMem){

        double finish=time+exec;

        System.out.printf(
        "%-7.1f %-9s %-8s %-4.0f %-6.0f %-7.1f %-6.1f %-6.1f %-7.1f %-8.0f %-8.0f %-8.0f %-8.0f %-7s\n",
        time,sched,"M-"+machine,cpu,mem,
        submit,think,exec,finish,
        availCpu,availMem,usedCpu,usedMem,
        "START");
    }

    static void finish(double time,String sched,int machine,double cpu,double mem,
                       double submit,double think,double exec,double start,
                       double availCpu,double availMem,double usedCpu,double usedMem){

        double finish=start+exec;

        System.out.printf(
        "%-7.1f %-9s %-8s %-4.0f %-6.0f %-7.1f %-6.1f %-6.1f %-7.1f %-8.0f %-8.0f %-8.0f %-8.0f %-7s\n",
        start,sched,"M-"+machine,cpu,mem,
        submit,think,exec,finish,
        availCpu,availMem,usedCpu,usedMem,
        "FINISH");
    }

    static void queued(double time,String sched,double submit,double think,
                       double availCpu,double availMem,double usedCpu,double usedMem){

        System.out.printf(
        "%-7s %-9s %-8s %-4s %-6s %-7.1f %-6.1f %-6s %-7s %-8.0f %-8.0f %-8.0f %-8.0f %-7s\n",
        "-",sched,"-","0","0",
        submit,think,"0","-",
        availCpu,availMem,usedCpu,usedMem,
        "QUEUED");
    }
}


// METRICS

class SchedulerMetrics{

    int successful;
    int failed;
    int retried;

    double thinkTime;
    double usefulTime;
    double wastedTime;
}


// CELL STATE

class CellState{

    int machines;
    double cpuPerMachine;
    double memPerMachine;

    double usedCpu=0;
    double usedMem=0;

    Map<String,Double> schedCpu=new HashMap<>();
    Map<String,Double> schedMem=new HashMap<>();

    CellState(int m,double c,double mem){

        machines=m;
        cpuPerMachine=c;
        memPerMachine=mem;
    }

    double totalCpu(){
        return machines*cpuPerMachine;
    }

    double totalMem(){
        return machines*memPerMachine;
    }

    double availableCpu(){
        return totalCpu()-usedCpu;
    }

    double availableMem(){
        return totalMem()-usedMem;
    }

    boolean allocate(String s,double cpu,double mem){

        if(availableCpu()>=cpu && availableMem()>=mem){

            usedCpu+=cpu;
            usedMem+=mem;

            schedCpu.merge(s,cpu,Double::sum);
            schedMem.merge(s,mem,Double::sum);

            return true;
        }

        return false;
    }

    void free(String s,double cpu,double mem){

        usedCpu-=cpu;
        usedMem-=mem;

        schedCpu.merge(s,-cpu,Double::sum);
        schedMem.merge(s,-mem,Double::sum);
    }

    int findMachine(){
        return (int)(Math.random()*machines);
    }
}


// JOB

class Job{

    static int idGen=0;

    int id;

    double cpu;
    double mem;
    double duration;
    double submit;

    int totalTasks;
    int unscheduledTasks;

    String workload="web";

    Job(double c,double m,double d,double s,int tasks){

        id=idGen++;

        cpu=c;
        mem=m;
        duration=d;
        submit=s;

        totalTasks=tasks;
        unscheduledTasks=tasks;
    }
}


// SCHEDULER

class Scheduler{

    String name;

    Queue<Job> queue=new LinkedList<>();

    ClusterSimulator sim;

    SchedulerMetrics metrics=new SchedulerMetrics();

    Map<String,Double> constantThinkTimes=Map.of("web",1.0);
    Map<String,Double> perTaskThinkTimes=Map.of("web",0.2);

    Scheduler(String name){
        this.name=name;
    }

    void addJob(Job j){

        queue.add(j);

        sim.allocator.requestOffer(this);
    }

    double getThinkTime(Job job){

        double c=constantThinkTimes.getOrDefault(job.workload,1.0);
        double p=perTaskThinkTimes.getOrDefault(job.workload,0.2);

        return c + p*job.unscheduledTasks;
    }

    void offer(){

        if(queue.isEmpty()) return;

        Job job=queue.peek();

        double think=getThinkTime(job);

        metrics.thinkTime+=think;

        if(sim.cell.allocate(name,job.cpu,job.mem)){

            metrics.successful++;
            metrics.usefulTime+=think;

            int machine=sim.cell.findMachine();

            EventLogger.start(
                    sim.time,name,machine,
                    job.cpu,job.mem,
                    job.submit,think,job.duration,
                    sim.cell.availableCpu(),
                    sim.cell.availableMem(),
                    sim.cell.usedCpu,
                    sim.cell.usedMem);

            sim.afterDelay(job.duration,()->{

                sim.cell.free(name,job.cpu,job.mem);

                EventLogger.finish(
                        sim.time,name,machine,
                        job.cpu,job.mem,
                        job.submit,think,job.duration,
                        sim.time-job.duration,
                        sim.cell.availableCpu(),
                        sim.cell.availableMem(),
                        sim.cell.usedCpu,
                        sim.cell.usedMem);

                sim.allocator.buildOffer();
            });

            job.unscheduledTasks--;

            if(job.unscheduledTasks==0)
                queue.poll();

        }else{

            metrics.failed++;
            metrics.retried++;
            metrics.wastedTime+=think;

            EventLogger.queued(
                    sim.time,name,job.submit,think,
                    sim.cell.availableCpu(),
                    sim.cell.availableMem(),
                    sim.cell.usedCpu,
                    sim.cell.usedMem);

            sim.afterDelay(1,()->addJob(job));

            queue.poll();
        }
    }
}


// DRF ALLOCATOR

class Allocator{

    ClusterSimulator sim;

    Set<Scheduler> requesting=new HashSet<>();

    void requestOffer(Scheduler s){

        requesting.add(s);

        sim.afterDelay(1,this::buildOffer);
    }

    double dominantShare(Scheduler s){

        double cpuShare=
                sim.cell.schedCpu.getOrDefault(s.name,0.0)
                        /sim.cell.totalCpu();

        double memShare=
                sim.cell.schedMem.getOrDefault(s.name,0.0)
                        /sim.cell.totalMem();

        return Math.max(cpuShare,memShare);
    }

    void buildOffer(){

        if(requesting.isEmpty()) return;

        List<Scheduler> list=new ArrayList<>(requesting);

        list.sort(Comparator.comparingDouble(this::dominantShare));

        Scheduler chosen=list.get(0);

        chosen.offer();
    }
}


// SIMULATOR

class ClusterSimulator{

    double time=0;

    PriorityQueue<SimEvent> events=new PriorityQueue<>();

    CellState cell;

    Allocator allocator=new Allocator();

    Map<String,Scheduler> schedulers=new HashMap<>();

    ClusterSimulator(CellState c){

        cell=c;

        allocator.sim=this;
    }

    void afterDelay(double d,Runnable a){

        events.add(new SimEvent(time+d,a));
    }

    void run(double until){

        while(!events.isEmpty()){

            SimEvent e=events.poll();

            if(e.time>until) break;

            time=e.time;

            e.action.run();
        }

        printMetrics();
    }

    void printMetrics(){

        int i=1;

        for(Scheduler s:schedulers.values()){

            SchedulerMetrics m=s.metrics;

            System.out.println();
            System.out.println("Scheduler "+i);

            System.out.println("Successful: "+m.successful);
            System.out.println("Failed: "+m.failed);
            System.out.println("Retried: "+m.retried);
            System.out.println("ThinkTime: "+m.thinkTime);
            System.out.println("UsefulTime: "+m.usefulTime);
            System.out.println("WastedTime: "+m.wastedTime);

            i++;
        }
    }
}


// MAIN

public class Main {

    public static void main(String[] args){

        EventLogger.header();

        CellState cell=new CellState(20,8,16000);

        ClusterSimulator sim=new ClusterSimulator(cell);

        Scheduler sA=new Scheduler("S-A");
        Scheduler sB=new Scheduler("S-B");

        sA.sim=sim;
        sB.sim=sim;

        sim.schedulers.put("S-A",sA);
        sim.schedulers.put("S-B",sB);

        Random r=new Random();

        for(int i=0;i<100;i++){

            Job j=new Job(
                    r.nextInt(3)+1,
                    512*(r.nextInt(2)+1),
                    r.nextInt(10)+5,
                    r.nextDouble()*5,
                    r.nextInt(4)+1);

            if(i%2==0)
                sim.afterDelay(j.submit,()->sA.addJob(j));
            else
                sim.afterDelay(j.submit,()->sB.addJob(j));
        }

        sim.run(500);
    }
}