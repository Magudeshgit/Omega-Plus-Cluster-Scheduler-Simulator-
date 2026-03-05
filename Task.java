package clusterscheduling;
public class Task {
     int cpuRequired;
    int memoryRequired;
    boolean isScheduled = false;

    public Task(int cpu, int memory) {
        this.cpuRequired = cpu;
        this.memoryRequired = memory;
    }
}
