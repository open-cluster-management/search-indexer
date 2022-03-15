// Copyright Contributors to the Open Cluster Management project

package config

import (
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func getKubeConfigPath() string {
	defaultKubePath := filepath.Join(os.Getenv("HOME"), ".kube", "config")
	if _, err := os.Stat(defaultKubePath); os.IsNotExist(err) {
		// set default to empty string if path does not reslove
		defaultKubePath = ""
	}

	kubeConfig := getEnv("KUBECONFIG", defaultKubePath)
	return kubeConfig
}

func getKubeConfig() *rest.Config {
	kubeConfigPath := getKubeConfigPath()
	var clientConfig *rest.Config
	var clientConfigError error

	if kubeConfigPath != "" {
		klog.Infof("Creating k8s client using KubeConfig at: %s", kubeConfigPath)
		clientConfig, clientConfigError = clientcmd.BuildConfigFromFlags("", kubeConfigPath)
	} else {
		klog.V(2).Info("Creating k8s client using InClusterClientConfig()")
		clientConfig, clientConfigError = rest.InClusterConfig()
	}

	if clientConfigError != nil {
		klog.Fatal("Error getting Kube Config: ", clientConfigError)
	}

	return clientConfig
}

func getKubeClient() *kubernetes.Clientset {
	config := getKubeConfig()
	var kubeClient *kubernetes.Clientset
	var err error
	if config != nil {
		kubeClient, err = kubernetes.NewForConfig(config)
		if err != nil {
			klog.Fatal("Cannot Construct Kube Client from Config: ", err)
		}
	} else {
		klog.Error("Cannot Construct Kube Client as input Config is nil")
	}
	return kubeClient
}
