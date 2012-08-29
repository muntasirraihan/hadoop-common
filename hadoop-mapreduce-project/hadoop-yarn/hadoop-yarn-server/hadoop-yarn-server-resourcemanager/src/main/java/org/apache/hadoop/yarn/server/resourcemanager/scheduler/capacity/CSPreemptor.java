package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
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
  private static final String SUSPEND_STRATEGY =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "suspend.strategy";
  private static final String SUSPEND_UNIT =
    CapacitySchedulerConfiguration.PREEMPT_PREFIX + "suspend.unit-mb";
  
  private static final long DEFAULT_INTERVAL_MS = 1000;
  private static final long DEFAULT_KILL_MS = 3000;
  private static final long DEFAULT_EXPIRE_MS = 6000;
  private static final float DEFAULT_UTILIZATION_TOL = 0.1f;
  private static final String DEFAULT_SUSPEND_STRATEGY = "random";
  private static final int DEFAULT_SUSPEND_UNIT = 512;

  private boolean suspend = false;
  private String suspendStrategy = DEFAULT_SUSPEND_STRATEGY;
  private int suspendUnit = DEFAULT_SUSPEND_UNIT;
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
    this.suspendStrategy = conf.get(SUSPEND_STRATEGY, DEFAULT_SUSPEND_STRATEGY);
    this.suspendUnit = conf.getInt(SUSPEND_UNIT, DEFAULT_SUSPEND_UNIT);
    
    LOG.info("(bcho2) kill interval "+killInterval);
    LOG.info("(bcho2) suspend strategy "+suspendStrategy);
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

  static class ResourcesComparator
  implements Comparator<SchedulerApp> {

    @Override
    public int compare(SchedulerApp o1, SchedulerApp o2) {
      return o1.getCurrentConsumption().compareTo(o2.getCurrentConsumption());
    }
  }
  
  private void releaseContainers(int releaseAmount, List<CSQueue> overCapList) {
    LOG.info("(bcho2) release containers: amount "+releaseAmount);
    for (CSQueue queue : overCapList) {
      List<SchedulerApp> releasableApplications =
        new ArrayList<SchedulerApp>(((LeafQueue)queue).getActiveApplications());
      if ("random".equals(suspendStrategy)) {
        releaseAmount =
          releaseContainersRandom(releaseAmount, releasableApplications);
      } else if ("probabilistic".equals(suspendStrategy)) {
        releaseAmount =
          releaseContainersProbabilistic(releaseAmount, releasableApplications);
      } else { // Use some kind of Comparator
        Comparator<SchedulerApp> comparator = null;
        if ("least-resources".equals(suspendStrategy)) {
          comparator = new ResourcesComparator();
        } else if ("most-resources".equals(suspendStrategy)) {
          comparator = Collections.reverseOrder(new ResourcesComparator());
        }
        Collections.sort(releasableApplications, comparator);
        releaseAmount = 
          releaseContainersOrdered(releaseAmount, releasableApplications);
      }
    }
    LOG.info("(bcho2) unassigned releaseAmount "+releaseAmount);
  }
  
  private int releaseContainersOrdered(int releaseAmount, List<SchedulerApp> releasableApplications) {
    for (SchedulerApp app : releasableApplications) {
      Resource consumption = app.getCurrentConsumption();
      int releasableMemory =
        consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                                      getMasterContainer().getResource().getMemory();
      // LOG.info("(bcho2) release containers: current consumption "+consumption+
      //     " app progress "+app.getRMApp().getProgress()+
      //     " app Master consumption "+app.getRMApp().getCurrentAppAttempt().getMasterContainer().getResource());
      // for (Priority prio : app.getPriorities()) {
      //   LOG.info("(bcho2) release containers: prio "+prio.getPriority()+
      //       " required resources "+app.getTotalRequiredResources(prio));
      // }
      if (releasableMemory >= releaseAmount) {
        app.addReleaseMemory(releaseAmount);
        return 0;
      } else {
        app.addReleaseMemory(releasableMemory);
        releaseAmount -= releasableMemory;
      }
    }
    return releaseAmount;
  }
  
  private int releaseContainersRandom(int releaseAmount, List<SchedulerApp> releasableApplications) {
    while(releasableApplications.size() > 0 && releaseAmount > 0) {
      Collections.shuffle(releasableApplications);
      Iterator<SchedulerApp> it = releasableApplications.iterator();
      while (it.hasNext()) {
        SchedulerApp app = it.next();
        Resource consumption = app.getCurrentConsumption();
        int releasableMemory =
          consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                                        getMasterContainer().getResource().getMemory();
        if (releasableMemory >= suspendUnit) {
          app.addReleaseMemory(suspendUnit);
          releaseAmount -= suspendUnit;
        } else {
          it.remove();
        }
      }
    }
    return releaseAmount;
  }

  Random random = new Random();
  private int releaseContainersProbabilistic(int releaseAmount, List<SchedulerApp> releasableApplications) {
    int totalReleasable = 0;
    LinkedHashMap<SchedulerApp, Integer> releasable =
      new LinkedHashMap<SchedulerApp, Integer>(releasableApplications.size());
    
    for (SchedulerApp app : releasableApplications) {
      Resource consumption = app.getCurrentConsumption();
      int releasableMemory = consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                             getMasterContainer().getResource().getMemory(); 
      releasable.put(app, releasableMemory);
      totalReleasable += releasableMemory;
    }

    while(totalReleasable > 0) {
      double rand = random.nextDouble();
      double lo;
      double hi = 0.0;
      Entry<SchedulerApp, Integer> entry = null;
      
      for (Entry<SchedulerApp, Integer> e : releasable.entrySet()) {
        lo = hi;
        hi += ((double)e.getValue())/totalReleasable;
        if (rand >= lo && rand < hi) { // Found our entry
          entry = e;
          break;
        }
      }
      if (entry == null) {
        LOG.error("(bcho2) no app chosen probabilistically!!");
        break;
      }
      
      SchedulerApp app = entry.getKey();
      int releasableMemory = entry.getValue();
      if (releasableMemory >= suspendUnit) {
        app.addReleaseMemory(suspendUnit);
        releaseAmount -= suspendUnit;
        totalReleasable -= suspendUnit;
      } else {
        entry.setValue(0);
        totalReleasable -= releasableMemory;
      }
    }
    return releaseAmount;
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
