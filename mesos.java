import java.util.*;

class Machine {

    int id;
    int totalCpu;
    int totalMem;
    int availableCpu;
    int availableMem;

    Machine(int id, int cpu, int mem) {
        this.id = id;
        this.totalCpu = cpu;
        this.totalMem = mem;
        this.availableCpu = cpu;
        this.availableMem = mem;
    }

    boolean allocate(int cpu, int mem) {
        if (availableCpu >= cpu && availableMem >= mem) {
            availableCpu -= cpu;
            availableMem -= mem;
            return true;
        }
        return false;
    }

    void free(int cpu, int mem) {
        availableCpu += cpu;
        availableMem += mem;
    }
}

class Task {

    int cpu;
    int mem;
    int duration;

    Task(int cpu, int mem, int duration) {
        this.cpu = cpu;
        this.mem = mem;
        this.duration = duration;
    }
}

class Job {

    int jobId;
    List<Task> tasks;

    Job(int id, List<Task> tasks) {
        this.jobId = id;
        this.tasks = tasks;
    }
}

class RandomJobGenerator {

    static Job generateJob(int jobId, int minutes) {

        Random rand = new Random();
        List<Task> tasks = new ArrayList<>();

        for (int m = 1; m <= minutes; m++) {

            int tasksThisMinute = rand.nextInt(11) + 10;

            for (int i = 0; i < tasksThisMinute; i++) {

                int cpu = rand.nextInt(4) + 1;
                int mem = (rand.nextInt(4) + 1) * 512;
                int duration = rand.nextInt(10) + 5;

                tasks.add(new Task(cpu, mem, duration));
            }
        }

        return new Job(jobId, tasks);
    }
}

class Event implements Comparable<Event> {

    double time;
    Runnable action;

    Event(double time, Runnable action) {
        this.time = time;
        this.action = action;
    }

    public int compareTo(Event e) {
        return Double.compare(this.time, e.time);
    }
}

class Simulator {

    PriorityQueue<Event> eventQueue = new PriorityQueue<>();
    double currentTime = 0;

    List<Machine> machines = new ArrayList<>();

    void start() {

        createMachines();

        Job job = RandomJobGenerator.generateJob(1, 100);

        System.out.println("Generated tasks: " + job.tasks.size());

        runEventLoop();
    }

    void createMachines() {

        machines.add(new Machine(1, 8, 8192));
        machines.add(new Machine(2, 8, 8192));
        machines.add(new Machine(3, 8, 8192));

        System.out.println("Machines created: " + machines.size());
    }

    void runEventLoop() {

        while (!eventQueue.isEmpty()) {

            Event e = eventQueue.poll();

            currentTime = e.time;

            e.action.run();
        }
    }

    void afterDelay(double seconds, Runnable action) {
        eventQueue.add(new Event(currentTime + seconds, action));
    }
}

public class Main {

    public static void main(String[] args) {

        Simulator sim = new Simulator();

        sim.start();
    }
}