package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;

// Bring back preemption. Based on what was stripped out with
//   HADOOP-5726 cs-without-preemption-23-1-2009.patch
//  (a) Tasks to Containers+Resources 
// Also takes ideas from Fair Scheduler port to 0.23.0
//   MAPREDUCE-3602 MAPREDUCE-3602.v1.patch
//  (a) use of SchedulerUtils
public class CSPreemptor implements Runnable { // TODO: make this abstract, create Suspend- and KillPreemptor

  protected static final Log LOG = LogFactory.getLog(CSPreemptor.class);
  
  private long preemptInterval;
  private long killInterval;
  private long expireInterval;
  private float utilizationTolerance;

  private static final String INTERVAL_MS =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "interval-ms";
  private static final String KILL_MS =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "kill-ms";
  private static final String EXPIRE_MS =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "expire-ms";
  private static final String UTILIZATION_TOL =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "utilization-tolerance";
  private static final String SUSPEND =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "suspend";
  
  private static final long DEFAULT_INTERVAL_MS = 1000;
  private static final long DEFAULT_KILL_MS = 3000;
  private static final long DEFAULT_EXPIRE_MS = 6000;
  private static final float DEFAULT_UTILIZATION_TOL = 0.1f;

  private boolean suspend = false;
  private boolean stopReclaim = false;
  private Clock clock = new SystemClock();
  
  protected CSQueue root;
  protected CapacitySchedulerContext scheduler;

  protected Map<CSQueue, Integer> reclaimingAmounts = 
    new HashMap<CSQueue, Integer>();
  protected Map<CSQueue, List<ReclaimedResource>> reclaimLists = 
    new HashMap<CSQueue, List<ReclaimedResource>>();
  protected Map<CSQueue, List<ReclaimedResource>> reclaimExpireLists = 
    new HashMap<CSQueue, List<ReclaimedResource>>();
  
  private static class ReclaimedResource {
    // how much resource to reclaim
    public int originalAmount;
    // how much is to be reclaimed currently
    public int currentAmount;
    // the time, in millisecs, when this object expires.
    // This time is equal to the time when the object was created, plus
    // the reclaim-time SLA for the queue.
    public long whenToExpire;
    // we also keep track of when to kill tasks, in millisecs. This is a
    // fraction of 'whenToExpire', but we store it here so we don't
    // recompute it every time.
    public long whenToKill;

    public ReclaimedResource(int amount, long expiryTime, long whenToKill) {
      this.originalAmount = amount;
      this.currentAmount = amount;
      this.whenToExpire = expiryTime;
      this.whenToKill = whenToKill;
    }
  }
  
  public void initialize(CSQueue root, CapacitySchedulerContext scheduler) {
    this.root = root;
    this.scheduler = scheduler;
    
    CapacitySchedulerConfiguration conf = scheduler.getConfiguration();
    this.preemptInterval = conf.getLong(INTERVAL_MS, DEFAULT_INTERVAL_MS);
    this.killInterval = conf.getLong(KILL_MS, DEFAULT_KILL_MS);
    this.expireInterval = conf.getLong(EXPIRE_MS, DEFAULT_EXPIRE_MS);
    this.utilizationTolerance = conf.getFloat(UTILIZATION_TOL, DEFAULT_UTILIZATION_TOL);
    this.suspend = conf.getBoolean(SUSPEND, false);
    
    LOG.info("(bcho2) kill interval "+killInterval);
  }
  
  public void run() {
    LOG.info("(bcho2) preemptor started, with interval "+preemptInterval);
    while (true) {
      try {
        Thread.sleep(preemptInterval);
        if (stopReclaim) {
          break;
        }
        reclaimCapacity();
      } catch (InterruptedException t) {
        break;
      } catch (Throwable t) {
        LOG.error("Error in redistributing capacity:\n", t);
      }
    }
  }
  
  /**
   * Call this when resources get allocated to queues.
   * e.g. when container is assigned in LeafQueue.assignContainers
   */
  public synchronized void updatePreemptor(LeafQueue leafQueue, Resource assigned) {
    List<ReclaimedResource> reclaimList = reclaimLists.get(leafQueue);
    int reclaimAmount = assigned.getMemory();
    LOG.info("(bcho2) updatedPreemptor: before reclaim amount "+reclaimAmount);
    if (reclaimingAmounts.containsKey(leafQueue)) {
      int amount = reclaimingAmounts.get(leafQueue);
      int updatedAmount = amount - reclaimAmount;
      LOG.info("(bcho2) after update preemptor updatedAmount "+updatedAmount);
      if (updatedAmount <= 0) {
        reclaimingAmounts.remove(leafQueue);
      } else {
        reclaimingAmounts.put(leafQueue, updatedAmount);
      }
    }
    if (reclaimList != null && !reclaimList.isEmpty()) {
      Iterator<ReclaimedResource> it = reclaimList.iterator();
      while(reclaimAmount > 0 && it.hasNext()) {
        ReclaimedResource reclaimedResource = it.next();
        if (reclaimAmount >= reclaimedResource.currentAmount) {
          reclaimAmount -= reclaimedResource.currentAmount;
          reclaimedResource.currentAmount = 0;
          // move to reclaimExpireLists -- doing it right?
          it.remove();
          addReclaimExpire(leafQueue, reclaimedResource);
        } else {
          reclaimedResource.currentAmount -= reclaimAmount;
          reclaimAmount = 0;
        }
      }
    }
    LOG.info("(bcho2) updatedPreemptor: after reclaim amount "+reclaimAmount);
  }
  
  public void reclaimCapacity() {
    // * Update lists:
    long currentTime = clock.getTime();
    // reclaim -> reclaimExpire
    int killAmount = 0;
    for (Entry<CSQueue, List<ReclaimedResource>> entry : reclaimLists.entrySet()) {
      Iterator<ReclaimedResource> it = entry.getValue().iterator();
      while(it.hasNext()) {
        ReclaimedResource reclaimedResource = it.next();
        LOG.debug("(bcho2)"
            +" whenToKill "+reclaimedResource.whenToKill
            +" currentTime "+currentTime);
        if (reclaimedResource.whenToKill < currentTime) {
          LOG.info("(bcho2) move to expire list, and add kill amount!"
              +" queue "+entry.getKey().getQueuePath()
              +" amount "+reclaimedResource.currentAmount);
          it.remove();
          addReclaimExpire(entry.getKey(), reclaimedResource);

          killAmount += reclaimedResource.currentAmount;
        }
      }
    }
    // reclaimExpire -> (removed)
    for (Entry<CSQueue, List<ReclaimedResource>> entry : reclaimExpireLists.entrySet()) {
      CSQueue queue = entry.getKey();
      Iterator<ReclaimedResource> it = entry.getValue().iterator();
      while(it.hasNext()) {
        ReclaimedResource reclaimedResource = it.next();
        LOG.debug("(bcho2)"
            +" whenToExpire "+reclaimedResource.whenToExpire
            +" currentTime "+currentTime);
        if (reclaimedResource.whenToExpire < currentTime) {
          LOG.info("(bcho2) remove from expire list"
              +" queue "+queue.getQueuePath()
              +" amount "+reclaimedResource.currentAmount);
          it.remove();
          
          if (reclaimingAmounts.containsKey(queue)) {
            int amount = reclaimingAmounts.get(queue);
            int updatedAmount = amount - reclaimedResource.currentAmount;
            LOG.info("(bcho2) after remove from expire list updatedAmount "+updatedAmount);
            if (updatedAmount <= 0) {
              reclaimingAmounts.remove(queue);
            } else {
              reclaimingAmounts.put(queue, updatedAmount);
            }
          }
        }
      }
    }
       
    
    // * Add to reclaimList
    // 1. Check queue caps. If no queues are over capacity, nothing to reclaim.
    List<CSQueue> overCapList =
      findQueueOverCap(root, new LinkedList<CSQueue>());
    if (overCapList.isEmpty()) {
      LOG.debug("(bcho2) no queues over cap");
      return;
    }
    for (CSQueue queue : overCapList) {
      LOG.info("(bcho2) queue "+queue.getQueuePath()+
          " over cap "+queue.getAbsoluteUsedCapacity());
    }
    
    // TODO: is there a better way?
    QueueMetrics metrics = root.getMetrics();
    int rootMB = 
      metrics.getAvailableMB() +
      metrics.getReservedMB() +
      metrics.getAllocatedMB();
    
    // 2. Check if anything to reclaim
    Map<CSQueue, List<Resource>> needMap =
      findQueueNeedResources(root, new HashMap<CSQueue, List<Resource>>());
    // Remove queues that are already over cap
    for (CSQueue queue : overCapList) {
      needMap.remove(queue);
    }
    if (needMap.isEmpty()) {
      LOG.debug("(bcho2) no queues need resources");
      return;      
    }
    int releaseAmount = 0;
    for (Entry<CSQueue, List<Resource>> needEntry : needMap.entrySet()) {
      int memNeeded = 0;
      CSQueue queue = needEntry.getKey();
      for (Resource resource : needEntry.getValue()) {
        memNeeded += resource.getMemory();
        LOG.info("(bcho2) queue "+queue.getQueuePath()+
          " needs resource "+resource.getMemory());
      }
      if (memNeeded == 0)
        continue;
      int memReclaiming = 0;
      float memReclaimingRatio = 0.0f;
      
      if (reclaimingAmounts.containsKey(queue)) {
        memReclaiming = reclaimingAmounts.get(queue);
        memReclaimingRatio = (float)memReclaiming / rootMB;
      }
      LOG.info("(bcho2) addIF"
          + " memNeeded " + memNeeded
          + " memReclaiming " + memReclaiming
          + " queueUsed " + queue.getAbsoluteUsedCapacity()
          + " memReclaimingRatio " + memReclaimingRatio
          + " queueCap " + queue.getAbsoluteCapacity());
      // If (need memory) && (not given capacity of memory), including what has been reclaimed
      if ((memNeeded - memReclaiming) > 0 &&
              (queue.getAbsoluteUsedCapacity() + memReclaimingRatio)
                < queue.getAbsoluteCapacity()) {
        // TODO while adding to the queue, also alert the AM's that some more resources are wanted
        addReclaim(queue,
            new ReclaimedResource(memNeeded-memReclaiming,
                currentTime+expireInterval,
                currentTime+killInterval));
        releaseAmount += (memNeeded-memReclaiming);
      }
    }
    
    // * Go through queues, tell AM to release resources
    if (suspend && releaseAmount > 0) {
      LOG.info("(bcho2) release amount "+releaseAmount);
      if (overCapList.size() > 0) {
        releaseContainers(releaseAmount, overCapList);
      }
    }
    // * Go through queues, kill containers (through NM)
    if (killAmount > 0) {
      LOG.info("(bcho2) kill amount "+killAmount);
      if (overCapList.size() > 0) {
        killContainers(killAmount, overCapList);
      }
    }
  }

  private void releaseContainers(int releaseAmount, List<CSQueue> overCapList) {
    for (CSQueue queue : overCapList) {
      for (SchedulerApp app : ((LeafQueue)queue).getActiveApplications()) {
        Resource consumption = app.getCurrentConsumption();
        LOG.info("(bcho2) release containers: current consumption "+consumption);
        if (consumption.getMemory() >= releaseAmount) {
          app.addReleaseMemory(releaseAmount);
          return;
        } else {
          app.addReleaseMemory(consumption.getMemory());
          releaseAmount -= consumption.getMemory();
        }
      }
    }
  }
  
  // TODO: sort this list, or otherwise evenly distribute the kills
  // TODO: not needed yet, since we only have a single research job
  private void killContainers(int killAmount, List<CSQueue> overCapList) {
    outerloop:
    for (CSQueue queue : overCapList) {
      for (SchedulerApp app : ((LeafQueue)queue).getActiveApplications()) {
        Resource consumption = app.getCurrentConsumption();
        LOG.info("(bcho2) kill containers: current consumption "+consumption);
        for (RMContainer container : app.getLiveContainers()) {
          if (killAmount <= 0) {
            break outerloop;
          }
          
          // TODO: should not be using magic variable! (bcho2)
          Priority priority = container.getContainer().getPriority();
          if (priority.getPriority() != 10) {
            LOG.info("(bcho2) skipping non-reduce priority "+priority.getPriority());
            continue;
          }

          killAmount -= container.getContainer().getResource().getMemory();
          LOG.info("(bcho2) kill container "+container.getContainerId()
              +" remaining kill amount "+killAmount);
          
          ContainerStatus status = 
            SchedulerUtils.createAbnormalContainerStatus(
                container.getContainerId(), SchedulerUtils.PREEMPT_KILLED_CONTAINER);
          scheduler.completedContainer(container, status, RMContainerEventType.KILL);
        }
      }
    }
  }
  
  private synchronized void addReclaim(CSQueue queue,
      ReclaimedResource reclaimedResource) {
    int memAmount = reclaimedResource.originalAmount;
    int previousAmount = 0;
    if (reclaimingAmounts.containsKey(queue))
      previousAmount = reclaimingAmounts.get(queue);
    reclaimingAmounts.put(queue, previousAmount + memAmount);
    
    List<ReclaimedResource> reclaimList = reclaimLists.get(queue);
    if (reclaimList == null) {
      reclaimList = new LinkedList<ReclaimedResource>();
      reclaimLists.put(queue, reclaimList);
    }
    reclaimList.add(reclaimedResource);

    LOG.info("(bcho2) add reclaimed resource, queue "+queue.getQueuePath()
        +" amount "+memAmount+" total amount "+(memAmount+previousAmount));
  }
  
  private synchronized void addReclaimExpire(CSQueue queue,
      ReclaimedResource reclaimedResource) {
    List<ReclaimedResource> reclaimExpireList = reclaimExpireLists.get(queue);
    if (reclaimExpireList == null) {
      reclaimExpireList = new LinkedList<ReclaimedResource>();
      reclaimExpireLists.put(queue, reclaimExpireList);
    }
    reclaimExpireList.add(reclaimedResource);

    LOG.info("(bcho2) add reclaimed expired resource, queue "+queue.getQueuePath());
  }

  // Recursive function
  private List<CSQueue> findQueueOverCap(CSQueue queue,
      List<CSQueue> overCapList) {
    List<CSQueue> children = queue.getChildQueues();
    if (children == null) { // LeafQueue
      if (queue.getAbsoluteUsedCapacity() >
            queue.getAbsoluteCapacity() * (1.0f + utilizationTolerance)) {
        overCapList.add(queue);
      }
    } else {
      for (CSQueue child : children) { // ParentQueue
        overCapList = findQueueOverCap(child, overCapList);
      }
    }
    return overCapList;
  }

  // Recursive function
  private Map<CSQueue,List<Resource>> findQueueNeedResources(CSQueue queue,
      Map<CSQueue, List<Resource>> needMap) {
    List<CSQueue> children = queue.getChildQueues();
    if (children == null) { // LeafQueue
      List<Resource> needList = ((LeafQueue)queue).needResources();
      if (!needList.isEmpty()) {
        needMap.put(queue, needList);
      }
    } else {
      for (CSQueue child : children) { // ParentQueue
        needMap = findQueueNeedResources(child, needMap);
      }
    }
    return needMap;
  }
}
