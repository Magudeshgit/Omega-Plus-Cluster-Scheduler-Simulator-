package mesos_stimulator;

import java.util.*;

class Event implements Comparable<Event>{

    double time;
    Runnable action;

    Event(double t,Runnable a){
        time=t;
        action=a;
    }

    public int compareTo(Event e){
        return Double.compare(time,e.time);
    }
}

class Machine{

    int id;
    int totalCpu;
    int totalMem;

    int availableCpu;
    int availableMem;

    Machine(int id,int cpu,int mem){

        this.id=id;
        totalCpu=cpu;
        totalMem=mem;

        availableCpu=cpu;
        availableMem=mem;
    }

    boolean allocate(int cpu,int mem){

        if(availableCpu>=cpu && availableMem>=mem){

            availableCpu-=cpu;
            availableMem-=mem;
            return true;
        }

        return false;
    }

    void free(int cpu,int mem){

        availableCpu+=cpu;
        availableMem+=mem;
    }
}

class Task{

    int cpu;
    int mem;

    int executionTime;

    double submitTime;

    Task(int cpu,int mem,int exec,double submit){

        this.cpu=cpu;
        this.mem=mem;
        executionTime=exec;
        submitTime=submit;
    }
}

class MesosSimulator{

    PriorityQueue<Event> events=new PriorityQueue<>();

    Queue<Task> waitingQueue=new LinkedList<>();

    List<Machine> machines=new ArrayList<>();

    double currentTime=0;

    int THINK_TIME=1;

    void initializeMachines(){

        int machineCount=5;

        for(int i=0;i<machineCount;i++)
            machines.add(new Machine(i,8,16384));

        System.out.println("Machines Generated : "+machineCount);
        System.out.println();

        System.out.println("Time Scheduler Machine CPU MEM Submit Think Exec Start Finish Event");
    }

    void tryAllocate(Task t,String scheduler){

        for(Machine m:machines){

            if(m.allocate(t.cpu,t.mem)){

                double start=currentTime;
                double finish=start+t.executionTime;

                log(currentTime,scheduler,"M-"+m.id,
                        t.cpu,t.mem,t.submitTime,
                        THINK_TIME,t.executionTime,start,finish,"START");

                afterDelay(t.executionTime,()->{

                    m.free(t.cpu,t.mem);

                    log(currentTime,scheduler,"M-"+m.id,
                            t.cpu,t.mem,t.submitTime,
                            THINK_TIME,t.executionTime,start,finish,"FINISH");

                    retryQueue();
                });

                return;
            }
        }

        waitingQueue.add(t);

        log(currentTime,scheduler,"-",
                t.cpu,t.mem,t.submitTime,
                THINK_TIME,t.executionTime,-1,-1,"QUEUED");
    }

    void retryQueue(){

        Iterator<Task> it=waitingQueue.iterator();

        while(it.hasNext()){

            Task t=it.next();

            afterDelay(THINK_TIME,()->tryAllocate(t,"QUEUE"));

            it.remove();
            break;
        }
    }

    void submitTask(Task t,String scheduler){

        afterDelay(THINK_TIME,()->tryAllocate(t,scheduler));
    }

    void log(double time,String scheduler,String machine,
             int cpu,int mem,double submit,
             int think,int exec,double start,double finish,String event){

        System.out.printf("%.1f %-8s %-6s %-3d %-4d %-6.1f %-5d %-4d %-5.1f %-6.1f %-6s\n",
                time,scheduler,machine,cpu,mem,
                submit,think,exec,start,finish,event);
    }

    void afterDelay(double delay,Runnable action){

        events.add(new Event(currentTime+delay,action));
    }

    void run(){

        while(!events.isEmpty()){

            Event e=events.poll();

            currentTime=e.time;

            e.action.run();
        }
    }

    void generateWorkload(){

        Random r=new Random();

        for(int i=0;i<20;i++){

            int cpu=r.nextInt(4)+1;
            int mem=(r.nextInt(4)+1)*512;
            int exec=r.nextInt(10)+5;

            Task t=new Task(cpu,mem,exec,i);

            submitTask(t,i%2==0?"S-A":"S-B");
        }
    }

    void start(){

        initializeMachines();

        generateWorkload();

        run();
    }
}

public class Main{

    public static void main(String[] args){

        new MesosSimulator().start();
    }
}