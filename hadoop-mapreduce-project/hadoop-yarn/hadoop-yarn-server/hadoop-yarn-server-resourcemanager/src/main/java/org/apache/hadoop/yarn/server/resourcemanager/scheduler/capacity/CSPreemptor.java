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
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;

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
  
  private static final long DEFAULT_INTERVAL_MS = 1000;
  private static final long DEFAULT_KILL_MS = 3000;
  private static final long DEFAULT_EXPIRE_MS = 6000;
  private static final float DEFAULT_UTILIZATION_TOL = 0.1f;
  private static final String DEFAULT_SUSPEND_STRATEGY = "random";

  private boolean suspend = false;
  private String suspendStrategy = DEFAULT_SUSPEND_STRATEGY;
  private boolean stopReclaim = false;
  private Clock clock = new SystemClock();
  
  protected CSQueue root;
  protected CapacitySchedulerContext scheduler;

  protected Map<CSQueue, Integer> reclaimingMemory = 
    new HashMap<CSQueue, Integer>();
  private void addReclaimingMemory(CSQueue queue, int memory) {
    int prev = 0;
    if (reclaimingMemory.containsKey(queue))
      prev = reclaimingMemory.get(queue);
    reclaimingMemory.put(queue, prev + memory);
  }
  private void subtractReclaimingMemory(CSQueue queue, int memory) {
    if (reclaimingMemory.containsKey(queue)) {
      int prev = reclaimingMemory.get(queue);
      int current = prev < memory ? 0 : prev - memory;
      reclaimingMemory.put(queue, current);
    }
  }
  
  protected Map<CSQueue, List<ReclaimedResource>> reclaimLists = 
    new HashMap<CSQueue, List<ReclaimedResource>>();
  protected Map<CSQueue, List<ReclaimedResource>> reclaimExpireLists = 
    new HashMap<CSQueue, List<ReclaimedResource>>();
  
  private final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);
  
  private static class ReclaimedResource {
    // how much resources still to reclaim
    private ResourceRequest currentResources;
    public final int originalContainers;
    // the time, in millisecs, when this object expires.
    // This time is equal to the time when the object was created, plus
    // the reclaim-time SLA for the queue.
    public long whenToExpire;
    // we also keep track of when to kill tasks, in millisecs. This is a
    // fraction of 'whenToExpire', but we store it here so we don't
    // recompute it every time.
    public long whenToKill;
    
    public ReclaimedResource(ResourceRequest resources, long expiryTime, long whenToKill) {
      this.currentResources = resources;
      this.originalContainers = resources.getNumContainers();
      this.whenToExpire = expiryTime;
      this.whenToKill = whenToKill;
    }
    
    public Resource getResource() {
      return currentResources.getCapability();
    }
    
    public void setNumContainers(int numContainers) {
      currentResources.setNumContainers(numContainers);
    }
    
    public int getNumContainers() {
      return currentResources.getNumContainers();
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
    LOG.info("(bcho2) updatePreemptor resource "+assigned);
    List<ReclaimedResource> reclaimList = reclaimLists.get(leafQueue);
    if (reclaimList == null) {
      LOG.info("(bcho2) updatePreemptor reclaimList null, return");
      return;
    }
    ReclaimedResource reclaimed = null;
    Iterator<ReclaimedResource> it = reclaimList.iterator();
    while (it.hasNext()) {
      ReclaimedResource rec = it.next();
      if (rec.getResource().equals(assigned)) {
        if (rec.getNumContainers() <= 0) {
          addReclaimExpire(leafQueue, rec);
          it.remove();
        } else {
          reclaimed = rec;
          break;
        }
      }
    }
    if (reclaimed == null) {
      LOG.info("(bcho2) updatePreemptor reclaimed null, return");
      return;
    }

    int numContainers = reclaimed.getNumContainers();
    // TODO: maybe go back to addReclaimExpire, with more reasonable timeouts
    // subtractReclaimingMemory(leafQueue, assigned.getMemory());
    numContainers--;
    reclaimed.setNumContainers(numContainers);
    LOG.info("(bcho2) updatePreemptor"+
        " reclaimed memory "+reclaimed.getResource().getMemory()+
    		" containers "+numContainers);
  }
  
  public void reclaimCapacity() {
    // * Update lists:
    long currentTime = clock.getTime();
    // reclaim -> reclaimExpire
    List<ReclaimedResource> killList = new ArrayList<ReclaimedResource>();
    int killAmount = 0;
    for (Entry<CSQueue, List<ReclaimedResource>> entry : reclaimLists.entrySet()) {
      Iterator<ReclaimedResource> it = entry.getValue().iterator();
      while(it.hasNext()) {
        ReclaimedResource reclaimedResource = it.next();
        LOG.debug("(bcho2)"
            +" whenToKill "+reclaimedResource.whenToKill
            +" currentTime "+currentTime);
        if (reclaimedResource.whenToKill < currentTime) {
          int memory = reclaimedResource.getNumContainers() * reclaimedResource.getResource().getMemory();
          LOG.info("(bcho2) move to expire list, and add kill amount!"
              +" queue "+entry.getKey().getQueuePath()
              +" memory "+memory);
          it.remove();
          addReclaimExpire(entry.getKey(), reclaimedResource);

          killAmount += memory;
          killList.add(reclaimedResource);
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
          int memory = reclaimedResource.originalContainers * reclaimedResource.getResource().getMemory();
          LOG.info("(bcho2) remove from expire list"
              +" queue "+queue.getQueuePath()
              +" memory "+memory);
          it.remove();
          subtractReclaimingMemory(queue, memory);
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
    Map<CSQueue, List<ResourceRequest>> needMap =
      findQueueNeedResources(root, new HashMap<CSQueue, List<ResourceRequest>>());
    // Remove queues that are already over cap
    for (CSQueue queue : overCapList) {
      needMap.remove(queue);
    }
    if (needMap.isEmpty()) {
      LOG.debug("(bcho2) no queues need resources");
      return;
    }
    for (Entry<CSQueue, List<ResourceRequest>> needEntry : needMap.entrySet()) {
      CSQueue queue = needEntry.getKey();
      for (ResourceRequest request : needEntry.getValue()) {
        int memNeeded = request.getCapability().getMemory() * request.getNumContainers();
        int memReclaiming = 0;
        float memReclaimingRatio = 0.0f;
        if (reclaimingMemory.containsKey(queue)) {
          memReclaiming = reclaimingMemory.get(queue);
          memReclaimingRatio = (float)memReclaiming / rootMB;
        }
        
        LOG.info("(bcho2) addIF"
            + " memNeeded " + memNeeded
            + " memReclaiming " + memReclaiming
            + " queue "+queue.getQueueName()
            + " queueUsed " + queue.getAbsoluteUsedCapacity()
            + " memReclaimingRatio " + memReclaimingRatio
            + " queueCap " + queue.getAbsoluteCapacity());

        // If (need memory) && (not given capacity of memory), including what has been reclaimed
        if ((memNeeded - memReclaiming) > 0 &&
                (queue.getAbsoluteUsedCapacity() + memReclaimingRatio)
                  < queue.getAbsoluteCapacity()) {
          // TODO while adding to the queue, also alert the AM's that some more resources are wanted
          addReclaim(queue,
              new ReclaimedResource(request,
                  currentTime+expireInterval,
                  currentTime+killInterval));
          releaseContainers(request, overCapList);
        }
      }
    }
    
    // * Go through queues, tell AM to release resources
    // This is now done above
//    if (suspend && releaseAmount > 0) {
//      LOG.info("(bcho2) release amount "+releaseAmount);
//      if (overCapList.size() > 0) {
//        releaseContainers(releaseAmount, overCapList);
//      }
//    }
    // * Go through queues, kill containers (through NM)    
    if (killAmount > 0) {
      LOG.info("(bcho2) kill amount "+killAmount+" but not killing!");
      
      if (overCapList.size() > 0) {
        for (ReclaimedResource rec : killList) {
          LOG.info("(bcho2) ignoring one last time "+rec.currentResources.getNumContainers()+
              "");
          if (needMap.size() == 0) {
            LOG.info("(bcho2) ignoring, needMap empty");
          }
          for (CSQueue need : needMap.keySet()) {
            LOG.info("(bcho2) ignoring, needMap: "+need.getQueueName()+
                " queueUsed " + need.getAbsoluteUsedCapacity()+
                " queueCap " + need.getAbsoluteCapacity()+
                " tolerance " + utilizationTolerance);
          }
          for (Entry<CSQueue, List<ReclaimedResource>> entry : reclaimLists.entrySet()) {
            List<ReclaimedResource> list = entry.getValue();
            if (list != null && list.size() > 0) {
              for (ReclaimedResource reclaim : list) {
                LOG.info("(bcho2) ignoring, reclaim: "+entry.getKey()+
                    " memory "+reclaim.currentResources.getCapability().getMemory()+
                    " containers "+reclaim.currentResources.getNumContainers());
              }
            }
          }
          
          // LOG.info("(bcho2) try release one last time "+rec.currentResources.getNumContainers());
          // releaseContainers(rec.currentResources, overCapList); 
        }
        // killContainers(killAmount, overCapList);
      }
    }
  }

  static class ResourcesComparator
  implements Comparator<SchedulerApp> {

    private Map<SchedulerApp, Resource> releasingConsumption = 
      new HashMap<SchedulerApp, Resource>();
    
    public void releaseConsumption(SchedulerApp app, int memory) {
      Resource release = Resources.createResource(memory);
      if (!releasingConsumption.containsKey(app)) {
        releasingConsumption.put(app, release);
      } else {
        releasingConsumption.put(app,
            Resources.addTo(releasingConsumption.get(app), release));
      }
    }
    
    @Override
    public int compare(SchedulerApp o1, SchedulerApp o2) {
      if (!releasingConsumption.containsKey(o1)) {
        releasingConsumption.put(o1, Resources.createResource(0));
      }
      if (!releasingConsumption.containsKey(o2)) {
        releasingConsumption.put(o2, Resources.createResource(0));
      }
      
      Resource o1r = Resources.subtract(o1.getCurrentConsumption(),
          releasingConsumption.get(o1));
      Resource o2r = Resources.subtract(o2.getCurrentConsumption(),
          releasingConsumption.get(o2));
      
      return o1r.compareTo(o2r);
    }
  }
  
  private void releaseContainers(ResourceRequest releaseRequest, List<CSQueue> overCapList) {
    LOG.info("(bcho2) release containers: memory "+releaseRequest.getCapability()+
        " num containers "+releaseRequest.getNumContainers());
    int releasedContainers = 0;
    for (CSQueue queue : overCapList) {
      List<SchedulerApp> releasableApplications =
        new ArrayList<SchedulerApp>(((LeafQueue)queue).getActiveApplications());
      if ("random".equals(suspendStrategy)) {
        releasedContainers =
          releaseContainersRandom(releaseRequest, releasableApplications);
      } else if ("probabilistic".equals(suspendStrategy)) {
        releasedContainers =
          releaseContainersProbabilistic(releaseRequest, releasableApplications);
      } else { // Use some kind of Comparator
        Comparator<SchedulerApp> comparator = null;
        if ("least-resources".equals(suspendStrategy)) {
          comparator = new ResourcesComparator();
        } else if ("most-resources".equals(suspendStrategy)) {
          comparator = Collections.reverseOrder(new ResourcesComparator());
        }
        releasedContainers = 
          releaseContainersOrderedIncremental(releaseRequest, releasableApplications, comparator);
        // TODO: A lot of repeated code here. Combine w/ probabilistic, by passing along the function (as an interface) that gets the next entry.
      }
    }
    LOG.info("(bcho2) number released "+releasedContainers);
  }
  
  private ResourceRequest createReleaseRequest(ResourceRequest baseRequest, int numContainers) {
    ResourceRequest request = BuilderUtils.newResourceRequest(baseRequest);
    request.setNumContainers(numContainers);
    return request;
  }

  private int releaseContainersOrdered(ResourceRequest releaseRequest, List<SchedulerApp> releasableApplications) {
    final int memoryPerContainer = releaseRequest.getCapability().getMemory();
    final int totalContainersToRelease = releaseRequest.getNumContainers();
    int totalContainersReleased = 0;
    for (SchedulerApp app : releasableApplications) {
      if (totalContainersReleased >= totalContainersToRelease)
        break;
      
      Resource consumption = app.getCurrentConsumption();
      int releasableMemory =
        consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                                      getMasterContainer().getResource().getMemory();
      
      int containersToRelease = (releasableMemory / memoryPerContainer) + (releasableMemory % memoryPerContainer == 0 ? 0 : 1);
      if (containersToRelease == 0) {
        continue;
      } else if (containersToRelease > totalContainersToRelease) {
        containersToRelease = totalContainersToRelease;
      }

      ResourceRequest appRequest = createReleaseRequest(releaseRequest, containersToRelease);
      app.addReleaseRequests(appRequest);
      
      totalContainersReleased += containersToRelease;
    }
    return totalContainersReleased;
  }

  private int releaseContainersOrderedIncremental(ResourceRequest releaseRequest,
      List<SchedulerApp> releasableApplications, Comparator<SchedulerApp> comparator) {
    final int memoryPerContainer = releaseRequest.getCapability().getMemory();
    final int totalContainersToRelease = releaseRequest.getNumContainers();
    int totalContainersReleased = 0;
    Map<SchedulerApp, Integer> containersMap =
      new HashMap<SchedulerApp, Integer>(releasableApplications.size());

    int totalReleasableContainers = 0;
    LinkedHashMap<SchedulerApp, Integer> releasable =
      new LinkedHashMap<SchedulerApp, Integer>(releasableApplications.size());
    
    for (SchedulerApp app : releasableApplications) {
      containersMap.put(app, 0);
      Resource consumption = app.getCurrentConsumption();
      int releasableMemory = consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                             getMasterContainer().getResource().getMemory();
      int releasableContainers = releasableMemory/memoryPerContainer;
      if (releasableContainers > 0) {
        releasable.put(app, releasableContainers);
        totalReleasableContainers += releasableContainers;
      }
    }

    while(totalContainersToRelease > totalContainersReleased &&
        totalReleasableContainers > 0) {
      if (releasableApplications.isEmpty()) {
        break;
      }
      Collections.sort(releasableApplications, comparator); // shouldn't be too much of a bottleneck, right?
      // But, just sorting on current consumption doesn't work...
      
      SchedulerApp app = releasableApplications.get(0);
      int releasableContainers = releasable.get(app);
      
      if (releasableContainers > 0) {
        containersMap.put(app, containersMap.get(app)+1);
        totalContainersReleased++;
        totalReleasableContainers--;
        
        ((ResourcesComparator)comparator).releaseConsumption(app, memoryPerContainer);
        
        LOG.info("(bcho2) releaseContainersOrderedIncremental app "+app+
            " containers "+containersMap.get(app));
        if (releasableContainers <= 1) {
          releasable.remove(app);
          releasableApplications.remove(0);
        } else {
          releasableContainers--;
          releasable.put(app, releasableContainers);
        }
      } else {
        LOG.error("(bcho2) releasableContainers "+releasableContainers+
            " for "+app);
      }
    }

    for (Entry<SchedulerApp, Integer> entry : containersMap.entrySet()) {
      SchedulerApp app = entry.getKey();
      int containersToRelease = entry.getValue();
      LOG.info("(bcho2) containersMap app "+app+
          " containersToRelease "+containersToRelease);
      ResourceRequest appRequest = createReleaseRequest(releaseRequest, containersToRelease);
      app.addReleaseRequests(appRequest);
    }
    return totalContainersReleased;
  }

  private int releaseContainersRandom(ResourceRequest releaseRequest, List<SchedulerApp> releasableApplications) {
    final int memoryPerContainer = releaseRequest.getCapability().getMemory();
    final int totalContainersToRelease = releaseRequest.getNumContainers();
    int totalContainersReleased = 0;
    Map<SchedulerApp, Integer> containersMap =
      new HashMap<SchedulerApp, Integer>(releasableApplications.size());
    
    while(releasableApplications.size() > 0 && totalContainersReleased < totalContainersToRelease) {
      Collections.shuffle(releasableApplications);
      Iterator<SchedulerApp> it = releasableApplications.iterator();
      while (it.hasNext()) {
        SchedulerApp app = it.next();
        Resource consumption = app.getCurrentConsumption();
        int releasableMemory =
          consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                                        getMasterContainer().getResource().getMemory();
        if (releasableMemory >= memoryPerContainer) {
          containersMap.put(app, containersMap.get(app)+1);
          totalContainersReleased++;
        } else {
          it.remove();
        }
      }
    }
    for (Entry<SchedulerApp, Integer> entry : containersMap.entrySet()) {
      SchedulerApp app = entry.getKey();
      int containersToRelease = entry.getValue();
      ResourceRequest appRequest = createReleaseRequest(releaseRequest, containersToRelease);
      app.addReleaseRequests(appRequest);
    }
    
    return totalContainersReleased;
  }

  Random random = new Random();
  private int releaseContainersProbabilistic(ResourceRequest releaseRequest, List<SchedulerApp> releasableApplications) {
    final int memoryPerContainer = releaseRequest.getCapability().getMemory();
    final int totalContainersToRelease = releaseRequest.getNumContainers();
    int totalContainersReleased = 0;
    Map<SchedulerApp, Integer> containersMap =
      new HashMap<SchedulerApp, Integer>(releasableApplications.size());
    
    int totalReleasableContainers = 0;
    LinkedHashMap<SchedulerApp, Integer> releasable =
      new LinkedHashMap<SchedulerApp, Integer>(releasableApplications.size());
    
    for (SchedulerApp app : releasableApplications) {
      containersMap.put(app, 0);
      Resource consumption = app.getCurrentConsumption();
      int releasableMemory = consumption.getMemory() - app.getRMApp().getCurrentAppAttempt().
                             getMasterContainer().getResource().getMemory();
      int releasableContainers = releasableMemory/memoryPerContainer;
      if (releasableContainers > 0) {
        releasable.put(app, releasableContainers);
        totalReleasableContainers += releasableContainers;
      }
    }

    while(totalContainersToRelease > totalContainersReleased &&
        totalReleasableContainers > 0) {
      double rand = random.nextDouble();
      double lo;
      double hi = 0.0;
      Entry<SchedulerApp, Integer> entry = null;
      
      for (Entry<SchedulerApp, Integer> e : releasable.entrySet()) {
        lo = hi;
        hi += ((double)e.getValue())/totalReleasableContainers;
        LOG.info("(bcho2) probabilistic app "+e.getKey().getApplicationId()+
            " lo "+lo+" hi "+hi+" rand "+rand);
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
      int releasableContainers = entry.getValue();
      
      if (releasableContainers > 0) {
        containersMap.put(app, containersMap.get(app)+1);
        totalContainersReleased++;
        totalReleasableContainers--;
        if (releasableContainers <= 1) {
          releasable.remove(app);
        } else {
          releasableContainers--;
          entry.setValue(releasableContainers);
        }
      } else {
        LOG.error("(bcho2) releasableContainers "+releasableContainers+
            " for "+app);
      }
    }
    for (Entry<SchedulerApp, Integer> entry : containersMap.entrySet()) {
      SchedulerApp app = entry.getKey();
      int containersToRelease = entry.getValue();
      ResourceRequest appRequest = createReleaseRequest(releaseRequest, containersToRelease);
      app.addReleaseRequests(appRequest);
    }
    return totalContainersReleased;
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
    int numContainers = reclaimedResource.getNumContainers();
    if (numContainers < 0) {
      LOG.warn("(bcho2) Should NOT be less than zero. Just leaving.");
      return;
    }
    addReclaimingMemory(queue, numContainers * reclaimedResource.getResource().getMemory());
    
    List<ReclaimedResource> reclaimList = reclaimLists.get(queue);
    if (reclaimList == null) {
      reclaimList = new LinkedList<ReclaimedResource>();
      reclaimLists.put(queue, reclaimList);
    }
    reclaimList.add(reclaimedResource);

    LOG.info("(bcho2) add reclaimed resource, queue "+queue.getQueuePath()
        +" amount "+numContainers+" total amount "+reclaimingMemory.get(queue));
  }
  
  private synchronized void addReclaimExpire(CSQueue queue,
      ReclaimedResource reclaimedResource) {
    int numContainers = reclaimedResource.getNumContainers();
    List<ReclaimedResource> reclaimExpireList = reclaimExpireLists.get(queue);
    if (reclaimExpireList == null) {
      reclaimExpireList = new LinkedList<ReclaimedResource>();
      reclaimExpireLists.put(queue, reclaimExpireList);
    }
    reclaimExpireList.add(reclaimedResource);

    LOG.info("(bcho2) add reclaimed expired resource, queue "+queue.getQueuePath()
        +" amount "+numContainers+" total amount "+reclaimingMemory.get(queue));
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
  private Map<CSQueue,List<ResourceRequest>> findQueueNeedResources(CSQueue queue,
      Map<CSQueue, List<ResourceRequest>> needMap) {
    List<CSQueue> children = queue.getChildQueues();
    if (children == null) { // LeafQueue
      List<ResourceRequest> needList = ((LeafQueue)queue).needResources();
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
