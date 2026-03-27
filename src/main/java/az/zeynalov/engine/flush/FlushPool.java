package az.zeynalov.engine.flush;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class FlushPool {
  private static final int THREAD_POOL_SIZE = 4;
  private final ExecutorService pool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

  public void submitFlushTask(Runnable task) {
    pool.submit(task);
  }

  public void shutdown() {
    pool.shutdown();
  }
}
