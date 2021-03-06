/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.api.data.DatasetContext;
import co.cask.cdap.api.macro.InvalidMacroException;
import co.cask.cdap.api.macro.MacroEvaluator;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.app.runtime.ProgramStateWriter;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.async.KeyedExecutor;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.common.logging.LoggingContext;
import co.cask.cdap.common.logging.LoggingContextAccessor;
import co.cask.cdap.common.service.Retries;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.data2.transaction.TransactionSystemClientAdapter;
import co.cask.cdap.data2.transaction.Transactions;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.internal.app.runtime.plugin.MacroParser;
import co.cask.cdap.internal.app.spark.SparkCompatReader;
import co.cask.cdap.internal.provision.task.DeprovisionTask;
import co.cask.cdap.internal.provision.task.ProvisionTask;
import co.cask.cdap.internal.provision.task.ProvisioningTask;
import co.cask.cdap.logging.context.LoggingContextHelper;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.SparkCompat;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import co.cask.cdap.runtime.spi.ssh.SSHContext;
import co.cask.cdap.security.spi.authentication.SecurityRequestContext;
import co.cask.cdap.security.tools.KeyStores;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Inject;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import org.apache.tephra.RetryStrategies;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.common.Cancellable;
import org.apache.twill.common.Threads;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nullable;

/**
 * Service for provisioning related operations
 */
public class ProvisioningService extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningService.class);
  private static final Logger SAMPLING_LOG = Loggers.sampling(LOG, LogSamplers.onceEvery(20));
  // Max out the HTTPS certificate validity.
  // We use max int of seconds to compute number of days to avoid overflow.
  private static final int CERT_VALIDITY_DAYS = (int) TimeUnit.SECONDS.toDays(Integer.MAX_VALUE);

  private final AtomicReference<ProvisionerInfo> provisionerInfo;
  private final ProvisionerProvider provisionerProvider;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final DatasetFramework datasetFramework;
  private final Transactional transactional;
  private final SparkCompat sparkCompat;
  private final SecureStore secureStore;
  private final Consumer<ProgramRunId> taskStateCleanup;
  private final ProgramStateWriter programStateWriter;
  private KeyedExecutor<ProvisioningTaskKey> taskExecutor;

  @Inject
  ProvisioningService(CConfiguration cConf, ProvisionerProvider provisionerProvider,
                      ProvisionerConfigProvider provisionerConfigProvider,
                      ProvisionerNotifier provisionerNotifier, LocationFactory locationFactory,
                      DatasetFramework datasetFramework, TransactionSystemClient txClient,
                      SecureStore secureStore, ProgramStateWriter programStateWriter) {
    this.provisionerProvider = provisionerProvider;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerNotifier = provisionerNotifier;
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
    this.locationFactory = locationFactory;
    this.datasetFramework = datasetFramework;
    this.transactional = Transactions.createTransactionalWithRetry(
      Transactions.createTransactional(new MultiThreadDatasetCache(new SystemDatasetInstantiator(datasetFramework),
                                                                   new TransactionSystemClientAdapter(txClient),
                                                                   NamespaceId.SYSTEM,
                                                                   Collections.emptyMap(), null, null)),
      RetryStrategies.retryOnConflict(20, 100)
    );
    this.sparkCompat = SparkCompatReader.get(cConf);
    this.secureStore = secureStore;
    this.programStateWriter = programStateWriter;
    this.taskStateCleanup = programRunId -> Transactionals.execute(transactional, dsContext -> {
      ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
      provisionerDataset.deleteTaskInfo(programRunId);
    });
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());
    reloadProvisioners();
    ExecutorService executorService = Executors.newCachedThreadPool(
      Threads.createDaemonThreadFactory("provisioning-service-%d"));
    this.taskExecutor = new KeyedExecutor<>(executorService);
    resumeTasks(taskStateCleanup);
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      // Wait for a moment for threads to complete. Even if they don't, however, it also ok since we have
      // the state persisted and the threads are daemon threads.
      taskExecutor.shutdownNow();
      taskExecutor.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  /**
   * Scans the ProvisionerDataset for any tasks that should be in progress but are not being executed and consumes
   * them.
   */
  @VisibleForTesting
  void resumeTasks(Consumer<ProgramRunId> taskCleanup) {
    // TODO: CDAP-13462 read tasks in chunks to avoid tx timeout
    List<ProvisioningTaskInfo> clusterTaskInfos = getInProgressTasks();

    for (ProvisioningTaskInfo provisioningTaskInfo : clusterTaskInfos) {
      State serviceState = state();
      // normally this is only called when the service is starting, but it can be running in unit test
      if (serviceState != State.STARTING && serviceState != State.RUNNING) {
        return;
      }
      ProvisioningTaskKey taskKey = provisioningTaskInfo.getTaskKey();
      Optional<Future<Void>> taskFuture = taskExecutor.getFuture(taskKey);
      if (taskFuture.isPresent()) {
        // don't resume the task if it's already been submitted.
        // this should only happen if a program run is started before we scanned for tasks
        continue;
      }
      ProgramRunId programRunId = provisioningTaskInfo.getProgramRunId();

      ProvisioningOp provisioningOp = provisioningTaskInfo.getProvisioningOp();
      String provisionerName = provisioningTaskInfo.getProvisionerName();
      Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);

      if (provisioner == null) {
        // can happen if CDAP is shut down in the middle of a task, and a provisioner is removed
        LOG.error("Could not provision cluster for program run {} because provisioner {} no longer exists.",
                  programRunId, provisionerName);
        provisionerNotifier.orphaned(programRunId);
        Transactionals.execute(transactional, dsContext -> {
          ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
          provisionerDataset.deleteTaskInfo(provisioningTaskInfo.getProgramRunId());
        });
        continue;
      }

      Runnable task;
      switch (provisioningOp.getType()) {
        case PROVISION:
          task = createProvisionTask(provisioningTaskInfo, provisioner);
          break;
        case DEPROVISION:
          task = createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
          break;
        default:
          LOG.error("Skipping unknown provisioning task type {}", provisioningOp.getType());
          continue;
      }

      // the actual task still needs to be run for cleanup to happen, but avoid logging a confusing
      // message about resuming a task that is in cancelled state.
      if (provisioningOp.getStatus() != ProvisioningOp.Status.CANCELLED) {
        LOG.info("Resuming provisioning task for run {} of type {} in state {}.",
                 provisioningTaskInfo.getProgramRunId(), provisioningTaskInfo.getProvisioningOp().getType(),
                 provisioningTaskInfo.getProvisioningOp().getStatus());
      }
      task.run();
    }
  }

  /**
   * Cancel the provision task for the program run. If there is no task or the task could not be cancelled before it
   * completed, an empty optional will be returned. If the task was cancelled, the state of the cancelled task will
   * be returned, and a message will be sent to mark the program run state as killed.
   *
   * @param programRunId the program run
   * @return the state of the task if it was cancelled
   */
  public Optional<ProvisioningTaskInfo> cancelProvisionTask(ProgramRunId programRunId) {
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION);
    return cancelTask(taskKey, programStateWriter::killed);
  }

  /**
   * Cancel the deprovision task for the program run. If there is no task or the task could not be cancelled before it
   * completed, an empty optional will be returned. If the task was cancelled, the state of the cancelled task will
   * be returned, and a message will be sent to mark the program run as orphaned.
   *
   * @param programRunId the program run
   * @return the state of the task if it was cancelled
   */
  public Optional<ProvisioningTaskInfo> cancelDeprovisionTask(ProgramRunId programRunId) {
    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.DEPROVISION);
    return cancelTask(taskKey, provisionerNotifier::orphaned);
  }

  /**
   * Record that a cluster will be provisioned for a program run, returning a Runnable that will actually perform
   * the cluster provisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   * Running the returned Runnable will start the actual task using an executor within this service so that it can be
   * tracked and optionally cancelled using {@link #cancelProvisionTask(ProgramRunId)}. The caller does not need to
   * submit the runnable using their own executor.
   *
   * @param provisionRequest the provision request
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster provisioning
   */
  public Runnable provision(ProvisionRequest provisionRequest, DatasetContext datasetContext) {
    ProgramRunId programRunId = provisionRequest.getProgramRunId();
    ProgramOptions programOptions = provisionRequest.getProgramOptions();
    Map<String, String> args = programOptions.getArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(args);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    // any errors seen here will transition the state straight to deprovisioned since no cluster create was attempted
    if (provisioner == null) {
      runWithProgramLogging(
        programRunId, args,
        () -> LOG.error("Could not provision cluster for the run because provisioner {} does not exist.", name));
      provisionerNotifier.deprovisioned(programRunId);
      return () -> { };
    }

    // Generate the SSH key pair if the provisioner is not YARN
    SecureKeyInfo secureKeyInfo = null;
    if (!YarnProvisioner.SPEC.equals(provisioner.getSpec())) {
      try {
        secureKeyInfo = generateSecureKeys(programRunId);
      } catch (Exception e) {
        runWithProgramLogging(
          programRunId, args,
          () -> LOG.error("Failed to generate keys for the program run with provisioner {}", name, e));
        provisionerNotifier.deprovisioned(programRunId);
        return () -> { };
      }
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(args);
    ProvisioningOp provisioningOp = new ProvisioningOp(ProvisioningOp.Type.PROVISION,
                                                       ProvisioningOp.Status.REQUESTING_CREATE);
    ProvisioningTaskInfo provisioningTaskInfo =
      new ProvisioningTaskInfo(programRunId, provisionRequest.getProgramDescriptor(), programOptions,
                               properties, name, provisionRequest.getUser(), provisioningOp, secureKeyInfo, null);
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
    provisionerDataset.putTaskInfo(provisioningTaskInfo);

    return createProvisionTask(provisioningTaskInfo, provisioner);
  }

  /**
   * Record that a cluster will be deprovisioned for a program run, returning a task that will actually perform
   * the cluster deprovisioning. This method must be run within a transaction.
   * The task returned should only be executed after the transaction that ran this method has completed.
   * Running the returned Runnable will start the actual task using an executor within this service so that it can be
   * tracked by this service. The caller does not need to submit the runnable using their own executor.
   *
   * @param programRunId the program run to deprovision
   * @param datasetContext dataset context for the transaction
   * @return runnable that will actually execute the cluster deprovisioning
   */
  public Runnable deprovision(ProgramRunId programRunId, DatasetContext datasetContext) {
    return deprovision(programRunId, datasetContext, taskStateCleanup);
  }

  // This is visible for testing, where we may not want to delete the task information after it completes
  @VisibleForTesting
  Runnable deprovision(ProgramRunId programRunId, DatasetContext datasetContext,
                       Consumer<ProgramRunId> taskCleanup) {
    ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);

    // look up information for the corresponding provision operation
    ProvisioningTaskInfo existing =
      provisionerDataset.getTaskInfo(new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION));
    if (existing == null) {
      runWithProgramLogging(programRunId, Collections.emptyMap(),
                            () -> LOG.error("No task state found while deprovisioning the cluster. "
                                              + "The cluster will be marked as orphaned."));
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }

    Provisioner provisioner = provisionerInfo.get().provisioners.get(existing.getProvisionerName());
    if (provisioner == null) {
      runWithProgramLogging(programRunId, existing.getProgramOptions().getArguments().asMap(),
                            () -> LOG.error("Could not deprovision the cluster because provisioner {} does not exist. "
                                              + "The cluster will be marked as orphaned.",
                                            existing.getProvisionerName()));
      provisionerNotifier.orphaned(programRunId);
      return () -> { };
    }

    ProvisioningOp provisioningOp = new ProvisioningOp(ProvisioningOp.Type.DEPROVISION,
                                                       ProvisioningOp.Status.REQUESTING_DELETE);
    ProvisioningTaskInfo provisioningTaskInfo = new ProvisioningTaskInfo(existing, provisioningOp,
                                                                         existing.getCluster());
    provisionerDataset.putTaskInfo(provisioningTaskInfo);

    return createDeprovisionTask(provisioningTaskInfo, provisioner, taskCleanup);
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed.
   */
  private void reloadProvisioners() {
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
      provisionerConfigProvider.loadProvisionerConfigs(provisioners.keySet());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      ProvisionerSpecification spec = provisionerEntry.getValue().getSpec();
      String provisionerName = provisionerEntry.getKey();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
                                                                 new ProvisionerConfig(new ArrayList<>()));
      details.put(provisionerName, new ProvisionerDetail(spec.getName(), spec.getLabel(), spec.getDescription(),
                                                         config.getConfigurationGroups()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  /**
   * @return unmodifiable collection of all provisioner specs
   */
  public Collection<ProvisionerDetail> getProvisionerDetails() {
    return provisionerInfo.get().details.values();
  }

  /**
   * Get the spec for the specified provisioner.
   *
   * @param name the name of the provisioner
   * @return the spec for the provisioner, or null if the provisioner does not exist
   */
  @Nullable
  public ProvisionerDetail getProvisionerDetail(String name) {
    return provisionerInfo.get().details.get(name);
  }

  /**
   * Validate properties for the specified provisioner.
   *
   * @param provisionerName the name of the provisioner to validate
   * @param properties properties for the specified provisioner
   * @throws NotFoundException if the provisioner does not exist
   * @throws IllegalArgumentException if the properties are invalid
   */
  public void validateProperties(String provisionerName, Map<String, String> properties) throws NotFoundException {
    Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);
    if (provisioner == null) {
      throw new NotFoundException(String.format("Provisioner '%s' does not exist", provisionerName));
    }
    provisioner.validateProperties(properties);
  }

  private Runnable createProvisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner) {
    ProgramRunId programRunId = taskInfo.getProgramRunId();

    ProvisionerContext context;
    try {
      context = createContext(programRunId, taskInfo.getUser(), taskInfo.getProvisionerProperties(),
                              createSSHContext(taskInfo.getSecureKeyInfo()));
    } catch (InvalidMacroException e) {
      runWithProgramLogging(taskInfo.getProgramRunId(), taskInfo.getProgramOptions().getArguments().asMap(),
                            () -> LOG.error("Could not evaluate macros while provisoning. "
                                              + "The run will be marked as failed.", e));
      provisionerNotifier.deprovisioned(taskInfo.getProgramRunId());
      return () -> { };
    }

    // TODO: (CDAP-13246) pick up timeout from profile instead of hardcoding
    ProvisioningTask task = new ProvisionTask(taskInfo, transactional, datasetFramework, provisioner, context,
                                              provisionerNotifier, 300);

    Runnable runnable = () -> runWithProgramLogging(
      programRunId, taskInfo.getProgramOptions().getArguments().asMap(),
      () -> {
        try {
          task.execute();
        } catch (InterruptedException e) {
          LOG.debug("Provision task for program run {} interrupted.", taskInfo.getProgramRunId());
        } catch (Exception e) {
          LOG.info("Provision task for program run {} failed.", taskInfo.getProgramRunId(), e);
        }
      });

    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(programRunId, ProvisioningOp.Type.PROVISION);
    return () -> taskExecutor.submit(taskKey, runnable);
  }

  private Runnable createDeprovisionTask(ProvisioningTaskInfo taskInfo, Provisioner provisioner,
                                         Consumer<ProgramRunId> taskCleanup) {
    Map<String, String> properties = taskInfo.getProvisionerProperties();
    ProvisionerContext context;
    try {
      context = createContext(taskInfo.getProgramRunId(), taskInfo.getUser(), properties,
                              createSSHContext(taskInfo.getSecureKeyInfo()));
    } catch (InvalidMacroException e) {
      runWithProgramLogging(taskInfo.getProgramRunId(), taskInfo.getProgramOptions().getArguments().asMap(),
                            () -> LOG.error("Could not evaluate macros while deprovisoning. "
                                              + "The cluster will be marked as orphaned.", e));
      provisionerNotifier.orphaned(taskInfo.getProgramRunId());
      return () -> { };
    }
    DeprovisionTask task = new DeprovisionTask(taskInfo, transactional, datasetFramework, 300,
                                               provisioner, context, provisionerNotifier, locationFactory);

    Runnable runnable = () -> runWithProgramLogging(
      taskInfo.getProgramRunId(), taskInfo.getProgramOptions().getArguments().asMap(),
      () -> {
        try {
          // any logs within the task, and any errors caused by the task will be logged in the program run context
          task.execute();
          taskCleanup.accept(taskInfo.getProgramRunId());
        } catch (InterruptedException e) {
          // We can get interrupted if the task is cancelled or CDAP is stopped. In either case, just return.
          // If it was cancelled, state cleanup is left to the caller. If it was CDAP master stopping, the task
          // will be resumed on master startup
          LOG.debug("Deprovision task for program run {} interrupted.", taskInfo.getProgramRunId());
        } catch (Exception e) {
          // Otherwise, if there was an error deprovisioning, run the cleanup
          LOG.info("Deprovision task for program run {} failed.", taskInfo.getProgramRunId(), e);
          taskCleanup.accept(taskInfo.getProgramRunId());
        }
      });

    ProvisioningTaskKey taskKey = new ProvisioningTaskKey(taskInfo.getProgramRunId(), ProvisioningOp.Type.DEPROVISION);
    return () -> taskExecutor.submit(taskKey, runnable);
  }

  private List<ProvisioningTaskInfo> getInProgressTasks() {
    return Retries.callWithRetries(
      () -> Transactionals.execute(transactional, dsContext -> {
        ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);
        return provisionerDataset.listTaskInfo();
      }),
      co.cask.cdap.common.service.RetryStrategies.fixDelay(6, TimeUnit.SECONDS),
      t -> {
        // don't retry if we were interrupted, or if the service is not running
        // normally this is only called when the service is starting, but it can be running in unit test
        State serviceState = state();
        if (serviceState != State.STARTING && serviceState != State.RUNNING) {
          return false;
        }
        if (t instanceof InterruptedException) {
          return false;
        }
        // Otherwise always retry, but log unexpected types of failures
        // We expect things like SocketTimeoutException or ConnectException
        // when talking to Dataset Service during startup
        Throwable rootCause = Throwables.getRootCause(t);
        if (!(rootCause instanceof SocketTimeoutException || rootCause instanceof ConnectException)) {
          SAMPLING_LOG.warn("Error scanning for in-progress provisioner tasks. " +
            "Tasks that were in progress during the last CDAP shutdown will not be resumed until this succeeds. ", t);
        }
        return true;
      });
  }

  /**
   * Generates {@link SecureKeyInfo} for later communication with the cluster.
   */
  private SecureKeyInfo generateSecureKeys(ProgramRunId programRunId) throws Exception {
    // Generate SSH key pair
    JSch jsch = new JSch();
    KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA, 2048);

    Location keysDir = locationFactory.create(String.format("provisioner/keys/%s.%s.%s.%s.%s",
                                                            programRunId.getNamespace(),
                                                            programRunId.getApplication(),
                                                            programRunId.getType().name().toLowerCase(),
                                                            programRunId.getProgram(),
                                                            programRunId.getRun()));
    keysDir.mkdirs();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePublicKey(bos, "cdap@cask.co");
    byte[] publicKey = bos.toByteArray();

    bos.reset();
    keyPair.writePrivateKey(bos, null);
    byte[] privateKey = bos.toByteArray();

    Location publicKeyFile = keysDir.append("id_rsa.pub");
    try (OutputStream os = publicKeyFile.getOutputStream()) {
      os.write(publicKey);
    }

    Location privateKeyFile = keysDir.append("id_rsa");
    try (OutputStream os = privateKeyFile.getOutputStream("600")) {
      os.write(privateKey);
    }

    // Generate KeyStores, one for server, one for client
    Location serverKeyStoreFile = generateKeyStore(keysDir.append("server.jks"));
    Location clientKeyStoreFile = generateKeyStore(keysDir.append("client.jks"));

    return new SecureKeyInfo(keysDir.toURI(), publicKeyFile.getName(), privateKeyFile.getName(),
                             serverKeyStoreFile.getName(), clientKeyStoreFile.getName(), "cdap");
  }

  /**
   * Generates a {@link KeyStore} that contains a private key and a self-signed public certificate pair and save
   * it to the given {@link Location}.
   */
  private Location generateKeyStore(Location outputLocation) throws Exception {
    KeyStore serverKeyStore = KeyStores.generatedCertKeyStore(CERT_VALIDITY_DAYS, "");
    try (OutputStream os = outputLocation.getOutputStream("600")) {
      serverKeyStore.store(os, "".toCharArray());
    }
    return outputLocation;
  }

  @Nullable
  private SSHContext createSSHContext(@Nullable SecureKeyInfo keyInfo) {
    return keyInfo == null ? null : new DefaultSSHContext(locationFactory, keyInfo);
  }

  private ProvisionerContext createContext(ProgramRunId programRunId, String userId, Map<String, String> properties,
                                           @Nullable SSHContext sshContext) {
    Map<String, String> evaluated = evaluateMacros(secureStore, userId, programRunId.getNamespace(), properties);
    return new DefaultProvisionerContext(programRunId, evaluated, sparkCompat, sshContext);
  }

  /**
   * Evaluates any macros in the property values and returns a copy of the map with the evaluations done.
   * Only the secure macro will be evaluated. Lookups and any other functions will be ignored and left as-is.
   * Secure store reads will be performed as the specified user, which should be the user that actually
   * run program code. This is required because this service runs in the CDAP master, which may be a different user
   * than the program user.
   *
   * @param secureStore the secure store to
   * @param user the user to perform secure store reads as
   * @param namespace the namespace of the program run
   * @param properties the properties to evaluate
   * @return a copy of the input properties with secure macros evaluated
   */
  @VisibleForTesting
  static Map<String, String> evaluateMacros(SecureStore secureStore, String user, String namespace,
                                            Map<String, String> properties) {
    // evaluate macros in the provisioner properties
    MacroEvaluator evaluator = new ProvisionerMacroEvaluator(namespace, secureStore);
    MacroParser macroParser = MacroParser.builder(evaluator)
      .disableEscaping()
      .disableLookups()
      .whitelistFunctions(ProvisionerMacroEvaluator.SECURE_FUNCTION)
      .build();
    Map<String, String> evaluated = new HashMap<>();

    // do secure store lookups as the user that will run the program
    String oldUser = SecurityRequestContext.getUserId();
    try {
      SecurityRequestContext.setUserId(user);
      for (Map.Entry<String, String> property : properties.entrySet()) {
        String key = property.getKey();
        String val = property.getValue();
        evaluated.put(key, macroParser.parse(val));
      }
    } finally {
      SecurityRequestContext.setUserId(oldUser);
    }
    return evaluated;
  }

  /**
   * Runs the runnable with the logging context set to the program run's context. Anything logged within the runnable
   * will be treated as if they were logs in the program run.
   *
   * @param programRunId the program run to log as
   * @param systemArgs system arguments for the run
   * @param runnable the runnable to run
   */
  private void runWithProgramLogging(ProgramRunId programRunId, Map<String, String> systemArgs, Runnable runnable) {
    LoggingContext loggingContext = LoggingContextHelper.getLoggingContextWithRunId(programRunId, systemArgs);
    Cancellable cancellable = LoggingContextAccessor.setLoggingContext(loggingContext);
    try {
      runnable.run();
    } finally {
      cancellable.cancel();
    }
  }

  /**
   * Cancel the task corresponding the the specified key. Returns true if the task was cancelled without completing.
   * Returns false if there was no task for the program run, or it completed before it could be cancelled.
   *
   * @param taskKey the key for the task to cancel
   * @return the state of the task when it was cancelled if the task was running
   */
  private Optional<ProvisioningTaskInfo> cancelTask(ProvisioningTaskKey taskKey, Consumer<ProgramRunId> onCancelled) {
    Optional<Future<Void>> taskFuture = taskExecutor.getFuture(taskKey);
    if (!taskFuture.isPresent()) {
      return Optional.empty();
    }

    Future<Void> future = taskFuture.get();
    if (future.isDone()) {
      return Optional.empty();
    }

    if (future.cancel(true)) {
      // this is the task state after it has been cancelled
      ProvisioningTaskInfo currentTaskInfo = Transactionals.execute(transactional, dsContext -> {
        ProvisionerDataset provisionerDataset = ProvisionerDataset.get(dsContext, datasetFramework);

        ProvisioningTaskInfo currentInfo = provisionerDataset.getTaskInfo(taskKey);
        if (currentInfo == null) {
          return null;
        }

        // write that the state has been cancelled. This is in case CDAP dies or is killed before the cluster can
        // be deprovisioned and the task state cleaned up. When CDAP starts back up, it will see that the task is
        // cancelled and will not resume the task.
        ProvisioningOp newOp =
          new ProvisioningOp(currentInfo.getProvisioningOp().getType(), ProvisioningOp.Status.CANCELLED);
        ProvisioningTaskInfo newTaskInfo = new ProvisioningTaskInfo(currentInfo, newOp, currentInfo.getCluster());
        provisionerDataset.putTaskInfo(newTaskInfo);

        return currentInfo;
      });

      // if the task state was gone by the time we cancelled, it's like we did not cancel it, since it's already
      // effectively done.
      if (currentTaskInfo == null) {
        return Optional.empty();
      }

      // run any action that should happen if it was cancelled, like writing a message to TMS
      onCancelled.accept(currentTaskInfo.getProgramRunId());
      return Optional.of(currentTaskInfo);
    }
    return Optional.empty();
  }

  /**
   * Just a container for provisioner instances and specs, so that they can be updated atomically.
   */
  private static class ProvisionerInfo {
    private final Map<String, Provisioner> provisioners;
    private final Map<String, ProvisionerDetail> details;

    private ProvisionerInfo(Map<String, Provisioner> provisioners, Map<String, ProvisionerDetail> details) {
      this.provisioners = Collections.unmodifiableMap(provisioners);
      this.details = Collections.unmodifiableMap(details);
    }
  }
}
