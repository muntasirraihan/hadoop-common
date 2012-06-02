package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.List;

public class KillPreemptor extends CSPreemptor {

  @Override
  public void reclaimCapacity() {
    logQueueStatus(root);
  }
  
  private void logQueueStatus(CSQueue queue) {
    LOG.info("(bcho2) "+queue.getQueuePath()+
        " utilization "+queue.getUtilization()+
        " capacity "+queue.getCapacity());
    List<CSQueue> children = queue.getChildQueues();
    if (children == null) { // LeafQueue
      // TODO get info about applications, not just print
      ((LeafQueue)queue).printApplicationResourceInfo();
    } else {
      for (CSQueue child : children) { // ParentQueue
        logQueueStatus(child);
      }
    }
  }
}
