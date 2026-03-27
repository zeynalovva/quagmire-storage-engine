package az.zeynalov.engine.memtable.exception;


public class ArenaCapacityException extends RuntimeException {

  private ArenaCapacityException(String message) {
    super(message);
  }

  public static ArenaCapacityException of(String message) {
    return new ArenaCapacityException(message);
  }
}
