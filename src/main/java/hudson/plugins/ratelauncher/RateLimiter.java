/**
 * {@link RetentionStrategy} that spins up a vm when needed and shuts it down when finished while respecting the hypervisor settings
 *
 * @author Jeffrey Lyman
 */

package hudson.plugins.ratelauncher;

import hudson.Extension;

import java.util.logging.Level;
import java.util.logging.Logger;

import hudson.model.labels.LabelAtom;
import hudson.slaves.RetentionStrategy;
import hudson.slaves.SlaveComputer;
import hudson.slaves.OfflineCause;
import hudson.model.*;
import hudson.model.Messages;
import jenkins.model.Jenkins;
import org.kohsuke.stapler.DataBoundConstructor;
import javax.annotation.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Collections;

public class RateLimiter extends RetentionStrategy<SlaveComputer> {
    private static final Logger LOGGER = Logger.getLogger(Demand.class.getName());
    private int maxSlaves;
    private LabelAtom label;

    @DataBoundConstructor
    public RateLimiter(String label, int maxSlaves){
        LOGGER.info("Libvirt RetentionStrategy constructed");
        this.maxSlaves = maxSlaves;
        this.label = Jenkins.getInstance().getLabelAtom(label);
    }

    @Override
    public boolean isManualLaunchAllowed(SlaveComputer vm){
        //count slaves in label
        return countLabel(label) < maxSlaves;
    }

    @Override
    @GuardedBy("hudson.model.Queue.lock")
    public long check(SlaveComputer vm) {
        if (vm.isOffline() && hasUniqueJob(vm) && countLabel(label) < maxSlaves){
            vm.connect(false);
        }
        return 1;
    }

    private static synchronized int countLabel(LabelAtom label){
        int count = 0;
        for (Computer o : Jenkins.getInstance().getComputers()) {
            if (o.isOnline() || o.isConnecting()) {
                if (o.getNode().getAssignedLabels().contains(label)){
                    count++;
               }
            }
        }
        return count;
    }

    private static synchronized boolean hasUniqueJob(Computer c){
        final HashMap<Computer, Integer> availableComputers = new HashMap<Computer, Integer>();
        for (Computer o : Jenkins.getInstance().getComputers()) {
            if ((o.isOnline() || o.isConnecting()) && o.isPartiallyIdle() && o.isAcceptingTasks()) {
                final int idleExecutors = o.countIdle();
                if (idleExecutors>0)
                    availableComputers.put(o, idleExecutors);
            }
        }

        for (Queue.BuildableItem item : Queue.getInstance().getBuildableItems()) {
            // can any of the currently idle executors take this task?
            // assume the answer is no until we can find such an executor
            boolean needExecutor = true;
            for (Computer o : Collections.unmodifiableSet(availableComputers.keySet())) {
                Node otherNode = o.getNode();
                if (otherNode != null && otherNode.canTake(item) == null) {
                    needExecutor = false;
                    final int availableExecutors = availableComputers.remove(o);
                    if (availableExecutors > 1) {
                        availableComputers.put(o, availableExecutors - 1);
                    } else {
                        availableComputers.remove(o);
                    }
                    break;
                }
            }

            // this 'item' cannot be built by any of the existing idle nodes, but it can be built by 'c'
            Node checkedNode = c.getNode();
            if (needExecutor && checkedNode != null && checkedNode.canTake(item) == null) {
                return true;
            }
        }
        return false;
    }


    @Extension
    public static class DescriptorImpl extends Descriptor<RetentionStrategy<?>> {
        @Override
        public String getDisplayName()  {
            return "Launch slave if the slave is in demand and the label isn't full";
        }
    }
}
