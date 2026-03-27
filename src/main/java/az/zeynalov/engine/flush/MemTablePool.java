package az.zeynalov.engine.flush;

import az.zeynalov.engine.memtable.MemTable;

public class MemTablePool {
  private static final int THRESHOLD = 4;
  private final MemTable[] memTables;

  public MemTablePool() {
    this.memTables = new MemTable[THRESHOLD];
  }

  public MemTable getAvailable(){
    for (int i = 0; i < THRESHOLD; i++) {
      if(memTables[i] != null){
        MemTable memTable = memTables[i];
        memTables[i] = null;
        return memTable;
      }
    }
    return null;
  }
}
