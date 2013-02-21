/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.yarn.Lock;
import org.apache.hadoop.yarn.Lock.NoLock;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.NodeType;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.security.ContainerTokenSecretManager;
import org.apache.hadoop.yarn.util.BuilderUtils;

@Private
@Unstable
public class LeafQueue implements CSQueue {
  private static final Log LOG = LogFactory.getLog(LeafQueue.class);

  private final String queueName;
  private CSQueue parent;
  private float capacity;
  private float absoluteCapacity;
  private float maximumCapacity;
  private float absoluteMaxCapacity;
  private float absoluteUsedCapacity = 0.0f;
  private int userLimit;
  private float userLimitFactor;

  private int maxApplications;
  private int maxApplicationsPerUser;
  
  private float maxAMResourcePerQueuePercent;
  private int maxActiveApplications; // Based on absolute max capacity
  private int maxActiveAppsUsingAbsCap; // Based on absolute capacity
  private int maxActiveApplicationsPerUser;
  
  private Resource usedResources = Resources.createResource(0);
  private float usedCapacity = 0.0f;
  private volatile int numContainers;
  
  /**
   * The sort order for the activeApplications determines what order they are
   * given assignments in. This order is determined by the CapacityScheduler,
   * which instantiates the LeafQueue with the appropriate comparator.
   */
  SortedSet<SchedulerApp> activeApplications;
  Map<ApplicationAttemptId, SchedulerApp> applicationsMap = 
      new HashMap<ApplicationAttemptId, SchedulerApp>();
  
  SortedSet<SchedulerApp> pendingApplications;
  
  private final Resource minimumAllocation;
  private final Resource maximumAllocation;
  private final float minimumAllocationFactor;

  private ContainerTokenSecretManager containerTokenSecretManager;

  private Map<String, User> users = new HashMap<String, User>();
  
  private final QueueMetrics metrics;

  private QueueInfo queueInfo; 

  private QueueState state;

  private Map<QueueACL, AccessControlList> acls = 
    new HashMap<QueueACL, AccessControlList>();
  
  private boolean resumeLocalOnly;

  private final RecordFactory recordFactory = 
    RecordFactoryProvider.getRecordFactory(null);

  private CapacitySchedulerContext scheduler;
  
  private final ActiveUsersManager activeUsersManager;
  
  /**
   * Create a LeafQueue.
   * @param cs the scheduler context
   * @param queueName name of this queue
   * @param parent parent in the queue tree
   * @param applicationComparator ordering of the applications for scheduling purposes
   * @param old
   */
  public LeafQueue(CapacitySchedulerContext cs, 
      String queueName, CSQueue parent, 
      Comparator<SchedulerApp> applicationComparator, CSQueue old) {
    this.scheduler = cs;
    this.queueName = queueName;
    this.parent = parent;
    // must be after parent and queueName are initialized
    this.metrics = old != null ? old.getMetrics() :
        QueueMetrics.forQueue(getQueuePath(), parent,
        cs.getConfiguration().getEnableUserMetrics());
    this.activeUsersManager = new ActiveUsersManager(metrics);
    this.minimumAllocation = cs.getMinimumResourceCapability();
    this.maximumAllocation = cs.getMaximumResourceCapability();
    this.minimumAllocationFactor = 
        (float)(maximumAllocation.getMemory() - minimumAllocation.getMemory()) / 
         maximumAllocation.getMemory();
    this.containerTokenSecretManager = cs.getContainerTokenSecretManager();

    float capacity = 
      (float)cs.getConfiguration().getCapacity(getQueuePath()) / 100;
    float absoluteCapacity = parent.getAbsoluteCapacity() * capacity;

    float maximumCapacity = 
        (float)cs.getConfiguration().getMaximumCapacity(getQueuePath()) / 100;
    float absoluteMaxCapacity = 
        CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);

    int userLimit = cs.getConfiguration().getUserLimit(getQueuePath());
    float userLimitFactor = 
      cs.getConfiguration().getUserLimitFactor(getQueuePath());

    int maxApplications = cs.getConfiguration().getMaximumApplicationsPerQueue(getQueuePath());
    if (maxApplications < 0) {
      int maxSystemApps = cs.getConfiguration().getMaximumSystemApplications();
      maxApplications = (int)(maxSystemApps * absoluteCapacity);
    }
    maxApplicationsPerUser = 
      (int)(maxApplications * (userLimit / 100.0f) * userLimitFactor);

    this.maxAMResourcePerQueuePercent = 
        cs.getConfiguration().
            getMaximumApplicationMasterResourcePerQueuePercent(getQueuePath());
    int maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            cs.getClusterResources(), this.minimumAllocation,
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    this.maxActiveAppsUsingAbsCap = 
            CSQueueUtils.computeMaxActiveApplications(
                cs.getClusterResources(), this.minimumAllocation,
                maxAMResourcePerQueuePercent, absoluteCapacity);
    int maxActiveApplicationsPerUser = 
        CSQueueUtils.computeMaxActiveApplicationsPerUser(maxActiveAppsUsingAbsCap, userLimit, 
            userLimitFactor);

    this.queueInfo = recordFactory.newRecordInstance(QueueInfo.class);
    this.queueInfo.setQueueName(queueName);
    this.queueInfo.setChildQueues(new ArrayList<QueueInfo>());

    QueueState state = cs.getConfiguration().getState(getQueuePath());

    Map<QueueACL, AccessControlList> acls = 
      cs.getConfiguration().getAcls(getQueuePath());

    this.resumeLocalOnly = cs.getConfiguration().getBoolean(YarnConfiguration.RESUME_LOCAL_ONLY,
                                                            YarnConfiguration.DEFAULT_RESUME_LOCAL_ONLY);
    
    setupQueueConfigs(
        cs.getClusterResources(),
        capacity, absoluteCapacity, 
        maximumCapacity, absoluteMaxCapacity, 
        userLimit, userLimitFactor, 
        maxApplications, maxApplicationsPerUser,
        maxActiveApplications, maxActiveApplicationsPerUser,
        state, acls);

    if(LOG.isDebugEnabled()) {
      LOG.debug("LeafQueue:" + " name=" + queueName
        + ", fullname=" + getQueuePath());
    }

    this.pendingApplications = 
        new TreeSet<SchedulerApp>(applicationComparator);
    this.activeApplications = new TreeSet<SchedulerApp>(applicationComparator);
  }

  private synchronized void setupQueueConfigs(
      Resource clusterResource,
      float capacity, float absoluteCapacity, 
      float maximumCapacity, float absoluteMaxCapacity,
      int userLimit, float userLimitFactor,
      int maxApplications, int maxApplicationsPerUser,
      int maxActiveApplications, int maxActiveApplicationsPerUser,
      QueueState state, Map<QueueACL, AccessControlList> acls)
  {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absCapacity = parent.getAbsoluteCapacity() * capacity;
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absCapacity, absoluteMaxCapacity);

    this.capacity = capacity; 
    this.absoluteCapacity = absCapacity;

    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absoluteMaxCapacity;

    this.userLimit = userLimit;
    this.userLimitFactor = userLimitFactor;

    this.maxApplications = maxApplications;
    this.maxApplicationsPerUser = maxApplicationsPerUser;

    this.maxActiveApplications = maxActiveApplications;
    this.maxActiveApplicationsPerUser = maxActiveApplicationsPerUser;
    
    this.state = state;

    this.acls = acls;

    this.queueInfo.setCapacity(this.capacity);
    this.queueInfo.setMaximumCapacity(this.maximumCapacity);
    this.queueInfo.setQueueState(this.state);

    StringBuilder aclsString = new StringBuilder();
    for (Map.Entry<QueueACL, AccessControlList> e : acls.entrySet()) {
      aclsString.append(e.getKey() + ":" + e.getValue().getAclString());
    }
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        this, parent, clusterResource, minimumAllocation);

    LOG.info("Initializing " + queueName + "\n" +
        "capacity = " + capacity +
        " [= (float) configuredCapacity / 100 ]" + "\n" + 
        "asboluteCapacity = " + absoluteCapacity +
        " [= parentAbsoluteCapacity * capacity ]" + "\n" +
        "maxCapacity = " + maximumCapacity +
        " [= configuredMaxCapacity ]" + "\n" +
        "absoluteMaxCapacity = " + absoluteMaxCapacity +
        " [= 1.0 maximumCapacity undefined, " +
        "(parentAbsoluteMaxCapacity * maximumCapacity) / 100 otherwise ]" + 
        "\n" +
        "userLimit = " + userLimit +
        " [= configuredUserLimit ]" + "\n" +
        "userLimitFactor = " + userLimitFactor +
        " [= configuredUserLimitFactor ]" + "\n" +
        "maxApplications = " + maxApplications +
        " [= configuredMaximumSystemApplicationsPerQueue or" + 
        " (int)(configuredMaximumSystemApplications * absoluteCapacity)]" + 
        "\n" +
        "maxApplicationsPerUser = " + maxApplicationsPerUser +
        " [= (int)(maxApplications * (userLimit / 100.0f) * " +
        "userLimitFactor) ]" + "\n" +
        "maxActiveApplications = " + maxActiveApplications +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) * " + 
        "maxAMResourcePerQueuePercent * absoluteMaxCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveAppsUsingAbsCap = " + maxActiveAppsUsingAbsCap +
        " [= max(" + 
        "(int)ceil((clusterResourceMemory / minimumAllocation) *" + 
        "maxAMResourcePercent * absoluteCapacity)," + 
        "1) ]" + "\n" +
        "maxActiveApplicationsPerUser = " + maxActiveApplicationsPerUser +
        " [= max(" +
        "(int)(maxActiveApplications * (userLimit / 100.0f) * " +
        "userLimitFactor)," +
        "1) ]" + "\n" +
        "usedCapacity = " + usedCapacity +
        " [= usedResourcesMemory / " +
        "(clusterResourceMemory * absoluteCapacity)]" + "\n" +
        "absoluteUsedCapacity = " + absoluteUsedCapacity +
        " [= usedResourcesMemory / clusterResourceMemory]" + "\n" +
        "maxAMResourcePerQueuePercent = " + maxAMResourcePerQueuePercent +
        " [= configuredMaximumAMResourcePercent ]" + "\n" +
        "minimumAllocationFactor = " + minimumAllocationFactor +
        " [= (float)(maximumAllocationMemory - minimumAllocationMemory) / " +
        "maximumAllocationMemory ]" + "\n" +
        "numContainers = " + numContainers +
        " [= currentNumContainers ]" + "\n" +
        "state = " + state +
        " [= configuredState ]" + "\n" +
        "acls = " + aclsString +
        " [= configuredAcls ]" + "\n");
  }
  
  @Override
  public synchronized float getCapacity() {
    return capacity;
  }

  @Override
  public synchronized float getAbsoluteCapacity() {
    return absoluteCapacity;
  }

  @Override
  public synchronized float getMaximumCapacity() {
    return maximumCapacity;
  }

  @Override
  public synchronized float getAbsoluteMaximumCapacity() {
    return absoluteMaxCapacity;
  }

  @Override
  public synchronized float getAbsoluteUsedCapacity() {
    return absoluteUsedCapacity;
  }

  @Override
  public CSQueue getParent() {
    return parent;
  }

  @Override
  public String getQueueName() {
    return queueName;
  }

  @Override
  public String getQueuePath() {
    return parent.getQueuePath() + "." + getQueueName();
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMinimumAllocation() {
    return minimumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public Resource getMaximumAllocation() {
    return maximumAllocation;
  }

  /**
   * Used only by tests.
   */
  @Private
  public float getMinimumAllocationFactor() {
    return minimumAllocationFactor;
  }

  public int getMaxApplications() {
    return maxApplications;
  }

  public synchronized int getMaxApplicationsPerUser() {
    return maxApplicationsPerUser;
  }

  public synchronized int getMaximumActiveApplications() {
    return maxActiveApplications;
  }

  public synchronized int getMaximumActiveApplicationsPerUser() {
    return maxActiveApplicationsPerUser;
  }

  @Override
  public ActiveUsersManager getActiveUsersManager() {
    return activeUsersManager;
  }

  @Override
  public synchronized float getUsedCapacity() {
    return usedCapacity;
  }

  @Override
  public synchronized Resource getUsedResources() {
    return usedResources;
  }

  @Override
  public List<CSQueue> getChildQueues() {
    return null;
  }

  @Override
  public synchronized void setUsedCapacity(float usedCapacity) {
    this.usedCapacity = usedCapacity;
  }

  @Override
  public synchronized void setAbsoluteUsedCapacity(float absUsedCapacity) {
    this.absoluteUsedCapacity = absUsedCapacity;
  }

  /**
   * Set maximum capacity - used only for testing.
   * @param maximumCapacity new max capacity
   */
  synchronized void setMaxCapacity(float maximumCapacity) {
    // Sanity check
    CSQueueUtils.checkMaxCapacity(getQueueName(), capacity, maximumCapacity);
    float absMaxCapacity = CSQueueUtils.computeAbsoluteMaximumCapacity(maximumCapacity, parent);
    CSQueueUtils.checkAbsoluteCapacities(getQueueName(), absoluteCapacity, absMaxCapacity);
    
    this.maximumCapacity = maximumCapacity;
    this.absoluteMaxCapacity = absMaxCapacity;
  }
  
  /**
   * Set user limit - used only for testing.
   * @param userLimit new user limit
   */
  synchronized void setUserLimit(int userLimit) {
    this.userLimit = userLimit;
  }

  /**
   * Set user limit factor - used only for testing.
   * @param userLimitFactor new user limit factor
   */
  synchronized void setUserLimitFactor(int userLimitFactor) {
    this.userLimitFactor = userLimitFactor;
  }

  synchronized void setParentQueue(CSQueue parent) {
    this.parent = parent;
  }
  
  @Override
  public synchronized int getNumApplications() {
    return getNumPendingApplications() + getNumActiveApplications();
  }

  public synchronized int getNumPendingApplications() {
    return pendingApplications.size();
  }

  public synchronized int getNumActiveApplications() {
    return activeApplications.size();
  }

  @Private
  public synchronized int getNumApplications(String user) {
    return getUser(user).getTotalApplications();
  }

  @Private
  public synchronized int getNumPendingApplications(String user) {
    return getUser(user).getPendingApplications();
  }

  @Private
  public synchronized int getNumActiveApplications(String user) {
    return getUser(user).getActiveApplications();
  }
  
  public synchronized int getNumContainers() {
    return numContainers;
  }

  @Override
  public synchronized QueueState getState() {
    return state;
  }

  @Private
  public synchronized int getUserLimit() {
    return userLimit;
  }

  @Private
  public synchronized float getUserLimitFactor() {
    return userLimitFactor;
  }

  @Override
  public synchronized Map<QueueACL, AccessControlList> getQueueAcls() {
    return new HashMap<QueueACL, AccessControlList>(acls);
  }

  @Override
  public synchronized QueueInfo getQueueInfo(
      boolean includeChildQueues, boolean recursive) {
    queueInfo.setCurrentCapacity(usedCapacity);
    return queueInfo;
  }

  @Override
  public synchronized List<QueueUserACLInfo> 
  getQueueUserAclInfo(UserGroupInformation user) {
    QueueUserACLInfo userAclInfo = 
      recordFactory.newRecordInstance(QueueUserACLInfo.class);
    List<QueueACL> operations = new ArrayList<QueueACL>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      }
    }

    userAclInfo.setQueueName(getQueueName());
    userAclInfo.setUserAcls(operations);
    return Collections.singletonList(userAclInfo);
  }

  public String toString() {
    return queueName + ": " + 
        "capacity=" + capacity + ", " + 
        "absoluteCapacity=" + absoluteCapacity + ", " + 
        "usedResources=" + usedResources.getMemory() + "MB, " + 
        "usedCapacity=" + getUsedCapacity() + ", " + 
        "absoluteUsedCapacity=" + getAbsoluteUsedCapacity() + ", " +
        "numApps=" + getNumApplications() + ", " + 
        "numContainers=" + getNumContainers();  
  }

  private synchronized User getUser(String userName) {
    User user = users.get(userName);
    if (user == null) {
      user = new User();
      users.put(userName, user);
    }
    return user;
  }

  @Override
  public synchronized void reinitialize(CSQueue queue, Resource clusterResource) 
  throws IOException {
    // Sanity check
    if (!(queue instanceof LeafQueue) || 
        !queue.getQueuePath().equals(getQueuePath())) {
      throw new IOException("Trying to reinitialize " + getQueuePath() + 
          " from " + queue.getQueuePath());
    }

    LeafQueue leafQueue = (LeafQueue)queue;
    setupQueueConfigs(
        clusterResource,
        leafQueue.capacity, leafQueue.absoluteCapacity, 
        leafQueue.maximumCapacity, leafQueue.absoluteMaxCapacity, 
        leafQueue.userLimit, leafQueue.userLimitFactor, 
        leafQueue.maxApplications,
        leafQueue.getMaxApplicationsPerUser(),
        leafQueue.getMaximumActiveApplications(), 
        leafQueue.getMaximumActiveApplicationsPerUser(),
        leafQueue.state, leafQueue.acls);
  }

  @Override
  public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    // Check if the leaf-queue allows access
    synchronized (this) {
      if (acls.get(acl).isUserAllowed(user)) {
        return true;
      }
    }

    // Check if parent-queue allows access
    return parent.hasAccess(acl, user);
  }
  
  public Set<SchedulerApp> getActiveApplications() {
    return Collections.unmodifiableSet(activeApplications);
  }
  
  @Override
  public void submitApplication(SchedulerApp application, String userName,
      String queue)  throws AccessControlException {
    // Careful! Locking order is important!

    // Check queue ACLs
    UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(userName);
    if (!hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi)) {
      throw new AccessControlException("User " + userName + " cannot submit" +
          " applications to queue " + getQueuePath());
    }

    User user = null;
    synchronized (this) {

      // Check if the queue is accepting jobs
      if (getState() != QueueState.RUNNING) {
        String msg = "Queue " + getQueuePath() +
        " is STOPPED. Cannot accept submission of application: " +
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for queues
      if (getNumApplications() >= getMaxApplications()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + getNumApplications() + " applications," +
        " cannot accept submission of application: " + 
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Check submission limits for the user on this queue
      user = getUser(userName);
      if (user.getTotalApplications() >= getMaxApplicationsPerUser()) {
        String msg = "Queue " + getQueuePath() + 
        " already has " + user.getTotalApplications() + 
        " applications from user " + userName + 
        " cannot accept submission of application: " + 
        application.getApplicationId();
        LOG.info(msg);
        throw new AccessControlException(msg);
      }

      // Add the application to our data-structures
      addApplication(application, user);
    }

    int attemptId = application.getApplicationAttemptId().getAttemptId();
    metrics.submitApp(userName, attemptId);

    // Inform the parent queue
    try {
      parent.submitApplication(application, userName, queue);
    } catch (AccessControlException ace) {
      LOG.info("Failed to submit application to parent-queue: " + 
          parent.getQueuePath(), ace);
      removeApplication(application, user);
      throw ace;
    }
  }

  private synchronized void activateApplications() {
    for (Iterator<SchedulerApp> i=pendingApplications.iterator(); 
         i.hasNext(); ) {
      SchedulerApp application = i.next();
      
      // Check queue limit
      if (getNumActiveApplications() >= getMaximumActiveApplications()) {
        break;
      }
      
      // Check user limit
      User user = getUser(application.getUser());
      if (user.getActiveApplications() < getMaximumActiveApplicationsPerUser()) {
        user.activateApplication();
        activeApplications.add(application);
        i.remove();
        LOG.info("Application " + application.getApplicationId() +
            " from user: " + application.getUser() + 
            " activated in queue: " + getQueueName());
      }
    }
  }
  
  private synchronized void addApplication(SchedulerApp application, User user) {
    // Accept 
    user.submitApplication();
    pendingApplications.add(application);
    applicationsMap.put(application.getApplicationAttemptId(), application);

    // Activate applications
    activateApplications();
    
    LOG.info("Application added -" +
        " appId: " + application.getApplicationId() +
        " user: " + user + "," + " leaf-queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }

  @Override
  public void finishApplication(SchedulerApp application, String queue) {
    // Careful! Locking order is important!
    synchronized (this) {
      removeApplication(application, getUser(application.getUser()));
    }

    // Inform the parent queue
    parent.finishApplication(application, queue);
  }

  public synchronized void removeApplication(SchedulerApp application, User user) {
    boolean wasActive = activeApplications.remove(application);
    if (!wasActive) {
      pendingApplications.remove(application);
    }
    applicationsMap.remove(application.getApplicationAttemptId());

    user.finishApplication(wasActive);
    if (user.getTotalApplications() == 0) {
      users.remove(application.getUser());
    }

    // Check if we can activate more applications
    activateApplications();
    
    // Inform the activeUsersManager
    synchronized (application) {
      activeUsersManager.deactivateApplication(
          application.getUser(), application.getApplicationId());
    }
    
    LOG.info("Application removed -" +
        " appId: " + application.getApplicationId() + 
        " user: " + application.getUser() + 
        " queue: " + getQueueName() +
        " #user-pending-applications: " + user.getPendingApplications() +
        " #user-active-applications: " + user.getActiveApplications() +
        " #queue-pending-applications: " + getNumPendingApplications() +
        " #queue-active-applications: " + getNumActiveApplications()
        );
  }
  
  private synchronized SchedulerApp getApplication(
      ApplicationAttemptId applicationAttemptId) {
    return applicationsMap.get(applicationAttemptId);
  }

  private static final CSAssignment NULL_ASSIGNMENT =
      new CSAssignment(Resources.createResource(0), NodeType.NODE_LOCAL);
  
  @Override
  public synchronized CSAssignment 
  assignContainers(Resource clusterResource, SchedulerNode node) {

    if(LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getHostName()
        + " #applications=" + activeApplications.size());
    }
    
    if (LOG.isDebugEnabled()) {
      for (SchedulerApp application : activeApplications) {
        LOG.debug("laxity for application " + application.getApplicationId() +
            " is " + application.getLaxity());
      }
    }
    
    // Check for reserved resources
    RMContainer reservedContainer = node.getReservedContainer();
    if (reservedContainer != null) {
      SchedulerApp application = 
          getApplication(reservedContainer.getApplicationAttemptId());
      LOG.info("(bcho2) assigning reserved container at"
          +" application="+application.getApplicationId()
          +" node="+node
          +" container="+reservedContainer.getContainerId()
          +" resource="+clusterResource);
      return new CSAssignment(
          assignReservedContainer(application, node, reservedContainer, 
              clusterResource),
          NodeType.NODE_LOCAL); // Don't care about locality constraints 
                                // for reserved containers
    }
    
    // Try to assign containers to applications in order
    for (SchedulerApp application : activeApplications) {
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("pre-assignContainers for application "
        + application.getApplicationId());
        application.showRequests();
      }

      synchronized (application) {
        // Schedule in priority order
        for (Priority priority : application.getPriorities()) {
          // Required resource
          Resource required = 
              application.getResourceRequest(priority, RMNode.ANY).getCapability();

          // Do we need containers at this 'priority'?
          if (!needContainers(application, priority, required)) {
            continue;
          }

          // Compute user-limit & set headroom
          // Note: We compute both user-limit & headroom with the highest 
          //       priority request as the target. 
          //       This works since we never assign lower priority requests
          //       before all higher priority ones are serviced.
          Resource userLimit = 
              computeUserLimitAndSetHeadroom(application, clusterResource, 
                  required);
          
          // Check queue max-capacity limit
          if (!assignToQueue(clusterResource, required)) {
            // instead of returning no assignment, send this resource request to
            // the preemptor for it to get us resources.
            LOG.debug("application " + application.getApplicationId() + " could not be assigned to a queue");
            return NULL_ASSIGNMENT;
          }
          
          // Check user limit
          if (!assignToUser(application.getUser(), userLimit)) {
            LOG.debug("could not assign, user is above limit");
            break; 
          }

          // Inform the application it is about to get a scheduling opportunity
          application.addSchedulingOpportunity(priority);
          
          // Try to schedule
          CSAssignment assignment =  
            assignContainersOnNode(clusterResource, node, application, priority, 
                null);

          // (bcho2)
          if (assignment == null) {
            LOG.info("(bcho2) assignment null,"
                +" node="+node
                +" application="+application.getApplicationId()
                +" priority="+priority.getPriority()
                +" totalContainersNeeded="+application.getTotalRequiredResources(priority));
            continue;
          }
          
          // Did we schedule or reserve a container?
          Resource assigned = assignment.getResource();

          // (bcho2)
          scheduler.getPreemptor().updatePreemptor(this, assigned);
          
          if (Resources.greaterThan(assigned, Resources.none())) {

            // Book-keeping 
            // Note: Update headroom to account for current allocation too...
            allocateResource(clusterResource, application, assigned);
            
            // Reset scheduling opportunities
            application.resetSchedulingOpportunities(priority);
            
            // Done
            LOG.info("(bcho2) assignment="+assignment
                +" node="+node
                +" application="+application.getApplicationId()
                +" priority="+priority.getPriority()
                +" totalContainersNeeded="+application.getTotalRequiredResources(priority));
            return assignment;
          } else {
            // Do not assign out of order w.r.t priorities
            LOG.info("(bcho2) assignment break,"
                +" node="+node
                +" application="+application.getApplicationId()
                +" priority="+priority.getPriority()
                +" totalContainersNeeded="+application.getTotalRequiredResources(priority));
            break;
          }
        }
      }

      if(LOG.isDebugEnabled()) {
        LOG.debug("post-assignContainers for application "
          + application.getApplicationId());
      }
      application.showRequests();
    }

    /*
    LOG.debug("(bcho2) assignment returning null,"
      +" node="+node);
    */
    return NULL_ASSIGNMENT;

  }

  private synchronized Resource assignReservedContainer(SchedulerApp application, 
      SchedulerNode node, RMContainer rmContainer, Resource clusterResource) {
    // Do we still need this reservation?
    Priority priority = rmContainer.getReservedPriority();
    if (application.getTotalRequiredResources(priority) == 0) {
      // Release
      Container container = rmContainer.getContainer();
      completedContainer(clusterResource, application, node, 
          rmContainer, 
          SchedulerUtils.createAbnormalContainerStatus(
              container.getId(), 
              SchedulerUtils.UNRESERVED_CONTAINER), 
          RMContainerEventType.RELEASED);
      return container.getResource(); // Ugh, return resource to force re-sort
    }

    // Try to assign if we have sufficient resources
    assignContainersOnNode(clusterResource, node, application, priority, 
        rmContainer);
    
    // Doesn't matter... since it's already charged for at time of reservation
    // "re-reservation" is *free*
    return org.apache.hadoop.yarn.server.resourcemanager.resource.Resource.NONE;
  }

  private synchronized boolean assignToQueue(Resource clusterResource, 
      Resource required) {
    // Check how of the cluster's absolute capacity we are currently using...
    float potentialNewCapacity = 
      (float)(usedResources.getMemory() + required.getMemory()) / 
        clusterResource.getMemory();
    if (potentialNewCapacity > absoluteMaxCapacity) {
      LOG.info(getQueueName() + 
          " usedResources: " + usedResources.getMemory() +
          " clusterResources: " + clusterResource.getMemory() +
          " currentCapacity " + ((float)usedResources.getMemory())/clusterResource.getMemory() + 
          " required " + required.getMemory() +
          " potentialNewCapacity: " + potentialNewCapacity + " ( " +
          " max-capacity: " + absoluteMaxCapacity + ")");
      return false;
    }
    return true;
  }

  @Lock({LeafQueue.class, SchedulerApp.class})
  private Resource computeUserLimitAndSetHeadroom(
      SchedulerApp application, Resource clusterResource, Resource required) {
    
    String user = application.getUser();
    
    /** 
     * Headroom is min((userLimit, queue-max-cap) - consumed)
     */

    Resource userLimit =                          // User limit
        computeUserLimit(application, clusterResource, required);
    

    Resource queueMaxCap =                        // Queue Max-Capacity
        Resources.createResource(
            CSQueueUtils.roundDown(minimumAllocation, 
                (int)(absoluteMaxCapacity * clusterResource.getMemory()))
            );
    
    Resource userConsumed = getUser(user).getConsumedResources(); 
    Resource headroom = 
        Resources.subtract(Resources.min(userLimit, queueMaxCap), userConsumed);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Headroom calculation for user " + user + ": " + 
          " userLimit=" + userLimit + 
          " queueMaxCap=" + queueMaxCap + 
          " consumed=" + userConsumed + 
          " headroom=" + headroom);
    }
    
    application.setHeadroom(headroom);
    metrics.setAvailableResourcesToUser(user, headroom);
    
    return userLimit;
  }
  
  @Lock(NoLock.class)
  private Resource computeUserLimit(SchedulerApp application, 
      Resource clusterResource, Resource required) {
    // What is our current capacity? 
    // * It is equal to the max(required, queue-capacity) if
    //   we're running below capacity. The 'max' ensures that jobs in queues
    //   with miniscule capacity (< 1 slot) make progress
    // * If we're running over capacity, then its
    //   (usedResources + required) (which extra resources we are allocating)

    // Allow progress for queues with miniscule capacity
    final int queueCapacity = 
      Math.max(
          CSQueueUtils.roundUp(
              minimumAllocation, 
              (int)(absoluteCapacity * clusterResource.getMemory())), 
          required.getMemory()
          );

    final int consumed = usedResources.getMemory();
    final int currentCapacity = 
      (consumed < queueCapacity) ? 
          queueCapacity : (consumed + required.getMemory());

    // Never allow a single user to take more than the 
    // queue's configured capacity * user-limit-factor.
    // Also, the queue's configured capacity should be higher than 
    // queue-hard-limit * ulMin
    
    final int activeUsers = activeUsersManager.getNumActiveUsers();  

    int limit = 
      CSQueueUtils.roundUp(
          minimumAllocation,
          Math.min(
              Math.max(divideAndCeil(currentCapacity, activeUsers), 
                       divideAndCeil((int)userLimit*currentCapacity, 100)),
              (int)(queueCapacity * userLimitFactor)
              )
          );

    if (LOG.isDebugEnabled()) {
      String userName = application.getUser();
      LOG.debug("User limit computation for " + userName + 
          " in queue " + getQueueName() +
          " userLimit=" + userLimit +
          " userLimitFactor=" + userLimitFactor +
          " required: " + required + 
          " consumed: " + getUser(userName).getConsumedResources() + 
          " limit: " + limit +
          " queueCapacity: " + queueCapacity + 
          " qconsumed: " + consumed +
          " currentCapacity: " + currentCapacity +
          " activeUsers: " + activeUsers +
          " clusterCapacity: " + clusterResource.getMemory()
      );
    }

    return Resources.createResource(limit);
  }
  
  private synchronized boolean assignToUser(String userName, Resource limit) {

    User user = getUser(userName);
    
    // Note: We aren't considering the current request since there is a fixed
    // overhead of the AM, but it's a > check, not a >= check, so... 
    if ((user.getConsumedResources().getMemory()) > limit.getMemory()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("User " + userName + " in queue " + getQueueName() + 
            " will exceed limit - " +  
            " consumed: " + user.getConsumedResources() + 
            " limit: " + limit
        );
      }
      return false;
    }

    return true;
  }

  static int divideAndCeil(int a, int b) {
    if (b == 0) {
      LOG.info("divideAndCeil called with a=" + a + " b=" + b);
      return 0;
    }
    return (a + (b - 1)) / b;
  }

  boolean needContainers(SchedulerApp application, Priority priority, Resource required) {
    int requiredContainers = application.getTotalRequiredResources(priority);
    int reservedContainers = application.getNumReservedContainers(priority);
    int starvation = 0;
    if (reservedContainers > 0) {
      float nodeFactor = 
          ((float)required.getMemory() / getMaximumAllocation().getMemory());
      
      // Use percentage of node required to bias against large containers...
      // Protect against corner case where you need the whole node with
      // Math.min(nodeFactor, minimumAllocationFactor)
      starvation = 
          (int)((application.getReReservations(priority) / (float)reservedContainers) * 
                (1.0f - (Math.min(nodeFactor, getMinimumAllocationFactor())))
               );
      
      if (LOG.isDebugEnabled()) {
        LOG.debug("needsContainers:" +
            " app.#re-reserve=" + application.getReReservations(priority) + 
            " reserved=" + reservedContainers + 
            " nodeFactor=" + nodeFactor + 
            " minAllocFactor=" + minimumAllocationFactor +
            " starvation=" + starvation +
            " finalvalue=" + ((starvation + requiredContainers) - reservedContainers) );
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("(bcho2) application "+application+
            " priority "+priority+
            " required "+required+
            " starvation "+starvation+
            " requiredContainers "+requiredContainers+
            " reservedContainers "+reservedContainers);
      }
    }
    return (((starvation + requiredContainers) - reservedContainers) > 0);
  }
  
  public List<ResourceRequest> needResources() {
    List<ResourceRequest> needList = new LinkedList<ResourceRequest>();
    for (SchedulerApp application : activeApplications) {
      synchronized (application) {
        for (Priority priority : application.getPriorities()) {
          // Required resource
          ResourceRequest request =
            application.getSavedRequest(priority, RMNode.ANY);
          Resource required = request.getCapability();

          if (request.getNumContainers() > 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("(bcho2) NEED resource "+required.getMemory()+
                  " containers "+request.getNumContainers()+
                  " app "+application.getApplicationId().toString()+
                  " prio "+priority.getPriority()+
                  " queue "+toString());
              }
            needList.add(request);
          }
          /*
          // Do we need containers at this 'priority'?
          if (needContainers(application, priority, required)) {
            LOG.info("(bcho2) need containers " +request.getNumContainers()+
              " app "+application.getApplicationId().toString()+
              " prio "+priority.getPriority()+
              " resource "+required.getMemory());
            needList.add(request);
          }
          */
        }
      }
    }
    return needList;
  }

  private CSAssignment assignContainersOnNode(Resource clusterResource, 
      SchedulerNode node, SchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {

    Resource assigned = Resources.none();

    // Data-local
    assigned = 
        assignNodeLocalContainers(clusterResource, node, application, priority,
            reservedContainer); 
    if (Resources.greaterThan(assigned, Resources.none())) {
      if (priority.getPriority() == 3) {
        LOG.info("(bcho2) NODE_LOCAL node "+node+" app "+application+" resources "+assigned.getMemory());
      }
      return new CSAssignment(assigned, NodeType.NODE_LOCAL);
    }

    // (bcho2) -- HACK
    if (resumeLocalOnly && priority.getPriority() == 3) {
      LOG.info("(bcho2) ignoring rack and off-switch, because resume local only");
      return null;
    }
    
    // Rack-local
    assigned = 
        assignRackLocalContainers(clusterResource, node, application, priority, 
            reservedContainer);
    if (Resources.greaterThan(assigned, Resources.none())) {
      if (priority.getPriority() == 3) {
        LOG.info("(bcho2) RACK_LOCAL node "+node+" app "+application+" resources "+assigned.getMemory());
      }
      return new CSAssignment(assigned, NodeType.RACK_LOCAL);
    }
    
    // Off-switch
    assigned =
      assignOffSwitchContainers(clusterResource, node, application, 
          priority, reservedContainer);
    if (Resources.greaterThan(assigned, Resources.none())) {
      if (priority.getPriority() == 3) {
        LOG.info("(bcho2) OFF_SWITCH node "+node+" app "+application+" resources "+assigned.getMemory());
      }
    }
    return new CSAssignment(assigned, NodeType.OFF_SWITCH);
  }

  private Resource assignNodeLocalContainers(Resource clusterResource, 
      SchedulerNode node, SchedulerApp application, 
      Priority priority, RMContainer reservedContainer) {
    ResourceRequest request = 
        application.getResourceRequest(priority, node.getHostName());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.NODE_LOCAL, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, 
            request, NodeType.NODE_LOCAL, reservedContainer);
      }
    }
    
    return Resources.none();
  }

  private Resource assignRackLocalContainers(Resource clusterResource,  
      SchedulerNode node, SchedulerApp application, Priority priority,
      RMContainer reservedContainer) {
    ResourceRequest request = 
      application.getResourceRequest(priority, node.getRackName());
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.RACK_LOCAL, 
          reservedContainer)) {
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.RACK_LOCAL, reservedContainer);
      }
    }
    return Resources.none();
  }

  private Resource assignOffSwitchContainers(Resource clusterResource, SchedulerNode node, 
      SchedulerApp application, Priority priority, 
      RMContainer reservedContainer) {
    ResourceRequest request = 
      application.getResourceRequest(priority, RMNode.ANY);
    if (request != null) {
      if (canAssign(application, priority, node, NodeType.OFF_SWITCH, 
          reservedContainer)) {
        if (priority.getPriority() == 3) {
          LOG.info("(bcho2) OFF_SWITCH node "+node+" app "+application);
        }
        return assignContainer(clusterResource, node, application, priority, request, 
            NodeType.OFF_SWITCH, reservedContainer);
      }
    }
    
    return Resources.none();
  }

  boolean canAssign(SchedulerApp application, Priority priority, 
      SchedulerNode node, NodeType type, RMContainer reservedContainer) {

    // Clearly we need containers for this application...
    if (type == NodeType.OFF_SWITCH) {
      if (reservedContainer != null) {
        return true;
      }

      // 'Delay' off-switch
      ResourceRequest offSwitchRequest = 
          application.getResourceRequest(priority, RMNode.ANY);
      long missedOpportunities = application.getSchedulingOpportunities(priority);
      long requiredContainers = offSwitchRequest.getNumContainers(); 
      
      float localityWaitFactor = 
        application.getLocalityWaitFactor(priority, 
            scheduler.getNumClusterNodes());
      
      return ((requiredContainers * localityWaitFactor) < missedOpportunities);
    }

    // Check if we need containers on this rack 
    ResourceRequest rackLocalRequest = 
      application.getResourceRequest(priority, node.getRackName());
    if (rackLocalRequest == null || rackLocalRequest.getNumContainers() <= 0) {
      return false;
    }

    // If we are here, we do need containers on this rack for RACK_LOCAL req
    if (type == NodeType.RACK_LOCAL) {
      return true;
    }

    // Check if we need containers on this host
    if (type == NodeType.NODE_LOCAL) {
      // Now check if we need containers on this host...
      ResourceRequest nodeLocalRequest = 
        application.getResourceRequest(priority, node.getHostName());
      if (nodeLocalRequest != null) {
        return nodeLocalRequest.getNumContainers() > 0;
      }
    }

    return false;
  }
  
  private Container getContainer(RMContainer rmContainer, 
      SchedulerApp application, SchedulerNode node, 
      Resource capability, Priority priority) {
    return (rmContainer != null) ? rmContainer.getContainer() :
      createContainer(application, node, capability, priority);
  }
  

  public Container createContainer(SchedulerApp application, SchedulerNode node, 
      Resource capability, Priority priority) {

    NodeId nodeId = node.getRMNode().getNodeID();
    ContainerId containerId = BuilderUtils.newContainerId(application
        .getApplicationAttemptId(), application.getNewContainerId());
    ContainerToken containerToken = null;

    // If security is enabled, send the container-tokens too.
    if (UserGroupInformation.isSecurityEnabled()) {
      containerToken =
          containerTokenSecretManager.createContainerToken(containerId, nodeId,
            capability);
      if (containerToken == null) {
        return null; // Try again later.
      }
    }

    // Create the container
    Container container = BuilderUtils.newContainer(containerId, nodeId,
        node.getRMNode().getHttpAddress(), capability, priority,
        containerToken);

    return container;
  }
  
  private Resource assignContainer(Resource clusterResource, SchedulerNode node, 
      SchedulerApp application, Priority priority, 
      ResourceRequest request, NodeType type, RMContainer rmContainer) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("assignContainers: node=" + node.getHostName()
        + " application=" + application.getApplicationId().getId()
        + " priority=" + priority.getPriority()
        + " request=" + request + " type=" + type);
    }
    Resource capability = request.getCapability();

    Resource available = node.getAvailableResource();

    assert (available.getMemory() >  0);

    // Create the container if necessary
    Container container = 
        getContainer(rmContainer, application, node, capability, priority);
  
    // something went wrong getting/creating the container 
    if (container == null) {
      return Resources.none();
    }

    // Can we allocate a container on this node?
    int availableContainers = 
        available.getMemory() / capability.getMemory();         
    if (availableContainers > 0) {
      // Allocate...

      // Did we previously reserve containers at this 'priority'?
      if (rmContainer != null){
        unreserve(application, priority, node, rmContainer);
      }

      // Inform the application
      RMContainer allocatedContainer = 
          application.allocate(type, node, priority, request, container);
      if (allocatedContainer == null) {
        // Did the application need this resource?
        return Resources.none();
      }

      // Inform the node
      node.allocateContainer(application.getApplicationId(), 
          allocatedContainer);

      LOG.info("assignedContainer" +
          " application=" + application.getApplicationId() +
          " container=" + container + 
          " containerId=" + container.getId() + 
          " queue=" + this + 
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources + 
          " cluster=" + clusterResource);

      return container.getResource();
    } else {
      if (priority.getPriority() == 0) { // HACK!!
        LOG.warn("(bcho2) NEVER reserve an AM slot, this could lead to never starting job; doing it anyway");
        // return Resources.none();
      }
      
      // Reserve by 'charging' in advance...
      reserve(application, priority, node, rmContainer, container);

      LOG.info("Reserved container " + 
          " application=" + application.getApplicationId() +
          " resource=" + request.getCapability() + 
          " queue=" + this.toString() + 
          " usedCapacity=" + getUsedCapacity() +
          " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
          " used=" + usedResources + 
          " cluster=" + clusterResource);

      return request.getCapability();
    }
  }

  private void reserve(SchedulerApp application, Priority priority, 
      SchedulerNode node, RMContainer rmContainer, Container container) {
    // Update reserved metrics if this is the first reservation
    if (rmContainer == null) {
      getMetrics().reserveResource(
          application.getUser(), container.getResource());
    }

    // Inform the application 
    rmContainer = application.reserve(node, priority, rmContainer, container);
    
    // Update the node
    node.reserveResource(application, priority, rmContainer);
  }

  private void unreserve(SchedulerApp application, Priority priority, 
      SchedulerNode node, RMContainer rmContainer) {
    // Done with the reservation?
    application.unreserve(node, priority);
    node.unreserveResource(application);
      
      // Update reserved metrics
    getMetrics().unreserveResource(
        application.getUser(), rmContainer.getContainer().getResource());
  }


  @Override
  public void completedContainer(Resource clusterResource, 
      SchedulerApp application, SchedulerNode node, RMContainer rmContainer, 
      ContainerStatus containerStatus, RMContainerEventType event) {
    if (application != null) {
      // Careful! Locking order is important!
      synchronized (this) {

        Container container = rmContainer.getContainer();
        
        // Inform the application & the node
        // Note: It's safe to assume that all state changes to RMContainer
        // happen under scheduler's lock... 
        // So, this is, in effect, a transaction across application & node
        if (rmContainer.getState() == RMContainerState.RESERVED) {
          unreserve(application, rmContainer.getReservedPriority(), 
              node, rmContainer);
        } else {
          application.containerCompleted(rmContainer, containerStatus, event);
          node.releaseContainer(container);
        }


        // Book-keeping
        releaseResource(clusterResource, 
            application, container.getResource());

        LOG.info("completedContainer" +
            " container=" + container +
            " resource=" + container.getResource() +
        		" queue=" + this + 
            " usedCapacity=" + getUsedCapacity() +
            " absoluteUsedCapacity=" + getAbsoluteUsedCapacity() +
            " used=" + usedResources + 
            " cluster=" + clusterResource);
      }

      // Inform the parent queue
      parent.completedContainer(clusterResource, application, 
          node, rmContainer, null, event);
    }
  }

  synchronized void allocateResource(Resource clusterResource, 
      SchedulerApp application, Resource resource) {
    // Update queue metrics
    Resources.addTo(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        this, parent, clusterResource, minimumAllocation);
    ++numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.assignContainer(resource);
    Resources.subtractFrom(application.getHeadroom(), resource); // headroom
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
    
    if (LOG.isDebugEnabled()) {
      LOG.info(getQueueName() + 
          " user=" + userName + 
          " used=" + usedResources + " numContainers=" + numContainers +
          " headroom = " + application.getHeadroom() +
          " user-resources=" + user.getConsumedResources()
          );
    }
  }

  synchronized void releaseResource(Resource clusterResource, 
      SchedulerApp application, Resource resource) {
    // Update queue metrics
    Resources.subtractFrom(usedResources, resource);
    CSQueueUtils.updateQueueStatistics(
        this, parent, clusterResource, minimumAllocation);
    --numContainers;

    // Update user metrics
    String userName = application.getUser();
    User user = getUser(userName);
    user.releaseContainer(resource);
    metrics.setAvailableResourcesToUser(userName, application.getHeadroom());
      
    LOG.info(getQueueName() + 
        " used=" + usedResources + " numContainers=" + numContainers + 
        " user=" + userName + " user-resources=" + user.getConsumedResources());
  }

  @Override
  public synchronized void updateClusterResource(Resource clusterResource) {
    // Update queue properties
    maxActiveApplications = 
        CSQueueUtils.computeMaxActiveApplications(
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteMaxCapacity);
    maxActiveAppsUsingAbsCap = 
        CSQueueUtils.computeMaxActiveApplications(
            clusterResource, minimumAllocation, 
            maxAMResourcePerQueuePercent, absoluteCapacity);
    maxActiveApplicationsPerUser = 
        CSQueueUtils.computeMaxActiveApplicationsPerUser(
            maxActiveAppsUsingAbsCap, userLimit, userLimitFactor);
    
    // Update metrics
    CSQueueUtils.updateQueueStatistics(
        this, parent, clusterResource, minimumAllocation);
    
    // Update application properties
    for (SchedulerApp application : activeApplications) {
      synchronized (application) {
        computeUserLimitAndSetHeadroom(application, clusterResource, 
            Resources.none());
      }
    }
  }
  
  @Override
  public QueueMetrics getMetrics() {
    return metrics;
  }

  static class User {
    Resource consumed = Resources.createResource(0);
    int pendingApplications = 0;
    int activeApplications = 0;

    public Resource getConsumedResources() {
      return consumed;
    }

    public int getPendingApplications() {
      return pendingApplications;
    }

    public int getActiveApplications() {
      return activeApplications;
    }

    public int getTotalApplications() {
      return getPendingApplications() + getActiveApplications();
    }
    
    public synchronized void submitApplication() {
      ++pendingApplications;
    }
    
    public synchronized void activateApplication() {
      --pendingApplications;
      ++activeApplications;
    }

    public synchronized void finishApplication(boolean wasActive) {
      if (wasActive) {
        --activeApplications;
      }
      else {
        --pendingApplications;
      }
    }

    public synchronized void assignContainer(Resource resource) {
      Resources.addTo(consumed, resource);
    }

    public synchronized void releaseContainer(Resource resource) {
      Resources.subtractFrom(consumed, resource);
    }
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApp application, Container container) {
    // Careful! Locking order is important! 
    synchronized (this) {
      allocateResource(clusterResource, application, container.getResource());
    }
    parent.recoverContainer(clusterResource, application, container);

  }
  
}
