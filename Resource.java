package clusterscheduling;

public class Resource {
    int machineId;
    int cpu;
    int memory;

    public Resource(int machineId, int cpu, int memory) {
        this.machineId = machineId;
        this.cpu = cpu;
        this.memory = memory;
    }

}
