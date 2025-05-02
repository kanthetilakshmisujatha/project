import java.util.*;
import java.util.concurrent.*;

public class WorkflowEngines {

    // Represents each task in the workflow
    private static class WorkTask {
        int id;
        String name;
        List<WorkTask> children = new LinkedList<>();
        List<WorkTask> parents = new LinkedList<>();
        int waitingForParents;
        
        WorkTask(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    public static void main(String[] args) throws Exception {
        // Read input and build the workflow
        Scanner scanner = new Scanner(System.in);
        
        // First read how many tasks we have
        int totalTasks = Integer.parseInt(scanner.nextLine());
        Map<Integer, WorkTask> tasks = new HashMap<>(totalTasks);
        
        // Read each task
        for (int i = 0; i < totalTasks; i++) {
            String[] parts = scanner.nextLine().split(":");
            int taskId = Integer.parseInt(parts[0]);
            String taskName = parts[1];
            tasks.put(taskId, new WorkTask(taskId, taskName));
        }
        
        // Read the dependencies between tasks
        int totalDependencies = Integer.parseInt(scanner.nextLine());
        for (int i = 0; i < totalDependencies; i++) {
            String[] edge = scanner.nextLine().split(":");
            int fromId = Integer.parseInt(edge[0]);
            int toId = Integer.parseInt(edge[1]);
            
            WorkTask parent = tasks.get(fromId);
            WorkTask child = tasks.get(toId);
            
            parent.children.add(child);
            child.parents.add(parent);
            child.waitingForParents++;
        }
        
        // Prepare for parallel execution
        ExecutorService workers = Executors.newFixedThreadPool(4);
        CountDownLatch allDone = new CountDownLatch(totalTasks);
        
        // Find the starting task (always ID 1)
        WorkTask startTask = tasks.get(1);
        if (startTask != null) {
            executeTask(startTask, workers, allDone);
        }
        
        // Wait for all tasks to complete
        allDone.await();
        workers.shutdown();
        
        // Print total task count
        System.out.println(totalTasks);
    }
    
    private static void executeTask(WorkTask task, ExecutorService workers, CountDownLatch doneSignal) {
        workers.submit(() -> {
            // First make sure all parents are done
            for (WorkTask parent : task.parents) {
                synchronized (parent) {
                    while (parent.waitingForParents != 0) {
                        try {
                            parent.wait();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                }
            }
            
            // Execute this task
            System.out.println(task.name);
            doneSignal.countDown();
            
            // Notify children that this parent is done
            synchronized (task) {
                task.waitingForParents = 0;
                task.notifyAll();
            }
            
            // Start all children that are ready
            for (WorkTask child : task.children) {
                synchronized (child) {
                    if (--child.waitingForParents == 0) {
                        executeTask(child, workers, doneSignal);
                    }
                }
            }
        });
    }
}