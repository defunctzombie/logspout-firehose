package raw

import (
	"testing"

	docker "github.com/fsouza/go-dockerclient"
)

func Test_extractKubernetesInfo(t *testing.T) {
	tests := []struct {
		name    string
		args    *docker.Container
		podName string
		fullPod string
	}{
		{"empty labels", container(), "", ""},
		{"deployment pods", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "tickettoride-5956cccbf9-d6fqn"), "tickettoride", "tickettoride-5956cccbf9-d6fqn"},
		{"statefulsets pods", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "smallworld3-0"), "smallworld3", "smallworld3-0"},
		{"statefulsets pods v2", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "metalobby-staging-1"), "metalobby-staging", "metalobby-staging-1"},
		{"deployment pods all figures", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "tickettoride-5956cccbf9-23456"), "tickettoride", "tickettoride-5956cccbf9-23456"},
		{"statefulsets pods 3 components", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "metalobby-pr-testing-2"), "metalobby-pr-testing", "metalobby-pr-testing-2"},
		{"deployment pods 3 components", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "tf-mars-beta-67754f879c-dsx84"), "tf-mars-beta", "tf-mars-beta-67754f879c-dsx84"},
		{"deployment pods 2 components", container("io.kubernetes.container.name", "blah", "io.kubernetes.pod.name", "tf-mars-66cc86c7cf-lwjlq"), "tf-mars", "tf-mars-66cc86c7cf-lwjlq"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractKubernetesInfo(tt.args)
			if got.Podprefix != tt.podName || got.Fullpod != tt.fullPod {
				t.Errorf("extractKubernetesInfo() = %v, want %v/%v", got, tt.podName, tt.fullPod)
			}
		})
	}
}

func container(el ...string) *docker.Container {
	labels := make(map[string]string)
	for i := 0; i < len(el); i += 2 {
		labels[el[i]] = el[i+1]
	}
	return &docker.Container{Config: &docker.Config{Labels: labels}}
}
