// Copyright Contributors to the Open Cluster Management project

package clustersync

import (
	"context"
	"time"

	"github.com/stolostron/search-indexer/pkg/config"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	klog "k8s.io/klog/v2"
)

func getNewLock(lockname, namespace string) *resourcelock.LeaseLock {

	client := config.GetKubeClient(config.GetKubeConfig()) // TODO: share the kube client.
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lockname,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: config.Cfg.PodName,
		},
	}
}

func runLeaderElection(lock *resourcelock.LeaseLock, ctx context.Context) {
	for { // TODO: How do I exit cleanly and remove the lock?
		leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
			Lock:            lock,
			ReleaseOnCancel: true,
			LeaseDuration:   30 * time.Second,
			RenewDeadline:   20 * time.Second,
			RetryPeriod:     2 * time.Second,
			Callbacks: leaderelection.LeaderCallbacks{
				OnStartedLeading: func(c context.Context) {
					doStuff()
				},
				OnStoppedLeading: func() {
					klog.Info("no longer the leader, staying inactive.")
					// TODO: need to stop doStuff()

				},
				OnNewLeader: func(current_id string) {
					if current_id == config.Cfg.PodName {
						klog.Info("I'm still the leader!")
						// TODO: Confirm that process is still running, restart if needed.
						return
					}
					klog.Infof("Leader is %s", current_id)
				},
			},
		})
		klog.Info("Restarting leader election loop.")
	}
}

func doStuff() {
	for {
		klog.Info("doing stuff...")
		time.Sleep(30 * time.Second)
	}
}
