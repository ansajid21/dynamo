/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package controller

import (
	"context"
	"errors"
	"testing"

	nvidiacomv1alpha1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1alpha1"
	nvidiacomv1beta1 "github.com/ai-dynamo/dynamo/deploy/operator/api/v1beta1"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/checkpoint"
	commonconsts "github.com/ai-dynamo/dynamo/deploy/operator/internal/consts"
	commonController "github.com/ai-dynamo/dynamo/deploy/operator/internal/controller_common"
	"github.com/ai-dynamo/dynamo/deploy/operator/internal/features"
	snapshotprotocol "github.com/ai-dynamo/dynamo/deploy/snapshot/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDynamoGraphDeploymentReconciler_selectWorkloadProgram(t *testing.T) {
	tests := []struct {
		name         string
		groveEnabled bool
		annotations  map[string]string
		wantProgram  workloadProgram
	}{
		{
			name:        "Grove feature disabled selects component program",
			wantProgram: &componentProgram{},
		},
		{
			name:         "Grove feature enabled selects Grove program",
			groveEnabled: true,
			wantProgram:  &groveProgram{},
		},
		{
			name:         "explicit Grove disable selects component program",
			groveEnabled: true,
			annotations: map[string]string{
				commonconsts.KubeAnnotationEnableGrove: commonconsts.KubeLabelValueFalse,
			},
			wantProgram: &componentProgram{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Log("Build the reconciler selection inputs")
			dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
				ObjectMeta: metav1.ObjectMeta{Annotations: tt.annotations},
			}
			reconciler := &DynamoGraphDeploymentReconciler{
				RuntimeConfig: &commonController.RuntimeConfig{
					Gate: features.Gates{Grove: tt.groveEnabled},
				},
			}

			t.Log("Select one complete workload program")
			got := reconciler.selectWorkloadProgram(dgd)

			assert.IsType(t, tt.wantProgram, got)
			if component, ok := got.(*componentProgram); ok {
				assert.Same(t, reconciler, component.reconciler)
			}
			if grove, ok := got.(*groveProgram); ok {
				assert.Same(t, reconciler, grove.reconciler)
			}
		})
	}
}

func TestNewWorkloadProgramResultCopiesStatus(t *testing.T) {
	dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
		Status: nvidiacomv1beta1.DynamoGraphDeploymentStatus{
			Checkpoints: map[string]nvidiacomv1beta1.ComponentCheckpointStatus{
				"worker": {},
			},
			RollingUpdate: &nvidiacomv1beta1.RollingUpdateStatus{
				Phase: nvidiacomv1beta1.RollingUpdatePhaseInProgress,
			},
		},
	}

	t.Log("Create a status accumulator independent from request.DGD.Status")
	result := newWorkloadProgramResult(dgd)
	require.NotNil(t, result.Status)
	result.Status.Checkpoints["decode"] = nvidiacomv1beta1.ComponentCheckpointStatus{}
	result.Status.RollingUpdate.Phase = nvidiacomv1beta1.RollingUpdatePhaseCompleted

	t.Log("Verify status accumulation does not mutate the request object")
	assert.NotContains(t, dgd.Status.Checkpoints, "decode")
	assert.Equal(t, nvidiacomv1beta1.RollingUpdatePhaseInProgress, dgd.Status.RollingUpdate.Phase)
}

func TestComponentProgram_ReconcilePreservesResultOnError(t *testing.T) {
	t.Log("Inject a component-path API failure before new status is produced")
	reconcileErr := errors.New("reconcile failed")
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return reconcileErr
			},
		}).
		Build()
	program := &componentProgram{
		reconciler: &DynamoGraphDeploymentReconciler{Client: kubeClient},
	}
	dgd := &nvidiacomv1beta1.DynamoGraphDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "graph", Namespace: "default"},
		Status: nvidiacomv1beta1.DynamoGraphDeploymentStatus{
			State: nvidiacomv1beta1.DGDStatePending,
			Components: map[string]nvidiacomv1beta1.ComponentReplicaStatus{
				"worker": {Replicas: 1},
			},
		},
	}
	previous := dgd.DeepCopy().Status

	result, err := program.Reconcile(context.Background(), workloadProgramRequest{DGD: dgd})

	t.Log("Verify the error result preserves prior status without mutating request.DGD.Status")
	require.ErrorIs(t, err, reconcileErr)
	require.NotNil(t, result.Status)
	assert.Equal(t, previous, *result.Status)
	assert.Equal(t, previous, dgd.Status)
	reason, ok := workloadProgramFailureReason(err)
	require.True(t, ok)
	assert.Equal(t, reasonFailedToInitializeWorkerHash, reason)
}

func TestGroveProgram_ReconcilePreservesResultOnError(t *testing.T) {
	t.Log("Inject an unsupported-path metadata failure before shared reconciliation")
	reconcileErr := errors.New("reconcile failed")
	dgd := createTestDGD("test-dgd", map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
		"worker": {ComponentType: commonconsts.ComponentTypeWorker},
	})
	kubeClient := fake.NewClientBuilder().
		WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
		WithObjects(dgd).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(context.Context, client.WithWatch, client.Object, ...client.UpdateOption) error {
				return reconcileErr
			},
		}).
		Build()
	program := &groveProgram{
		reconciler: &DynamoGraphDeploymentReconciler{
			Client:   kubeClient,
			Recorder: record.NewFakeRecorder(10),
		},
	}
	dgd.Status = nvidiacomv1beta1.DynamoGraphDeploymentStatus{
		State: nvidiacomv1beta1.DGDStatePending,
		Components: map[string]nvidiacomv1beta1.ComponentReplicaStatus{
			"worker": {Replicas: 1},
		},
	}
	previous := dgd.DeepCopy().Status

	result, err := program.Reconcile(context.Background(), workloadProgramRequest{DGD: dgd})

	t.Log("Verify failed primary mutation does not mutate request.DGD.Status")
	require.ErrorIs(t, err, reconcileErr)
	require.NotNil(t, result.Status)
	assert.Equal(t, previous, *result.Status)
	assert.Equal(t, previous, dgd.Status)
	reason, ok := workloadProgramFailureReason(err)
	require.True(t, ok)
	assert.Equal(t, reasonFailedToInitializeWorkerHash, reason)
}

func TestComponentProgram_ReconcileReturnsPartialRolloutStatusOnLaterError(t *testing.T) {
	t.Log("Build a worker change that starts rollout before shared input reconciliation")
	dgd := createTestDGD("test-dgd", map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
		"worker": {
			ComponentType: commonconsts.ComponentTypeWorker,
			Envs:          []corev1.EnvVar{{Name: "WORKER_VERSION", Value: "new"}},
		},
	})
	dgd.Annotations = map[string]string{
		commonconsts.AnnotationCurrentWorkerHash: "old-worker-hash",
	}
	reconciler := createTestReconcilerWithStatus(dgd)
	program := &componentProgram{reconciler: reconciler}

	result, err := program.Reconcile(context.Background(), workloadProgramRequest{DGD: dgd})

	t.Log("Verify rollout status is returned on the later shared-input failure")
	require.ErrorContains(t, err, "RBAC manager not initialized")
	require.NotNil(t, result.Status)
	require.NotNil(t, result.Status.RollingUpdate)
	assert.Equal(t, nvidiacomv1beta1.RollingUpdatePhasePending, result.Status.RollingUpdate.Phase)
	assert.Nil(t, dgd.Status.RollingUpdate)
}

func TestComponentProgram_ReconcileWorkerRollout(t *testing.T) {
	t.Run("single-node component workload starts a managed rollout", func(t *testing.T) {
		dgd := createTestDGD("test-dgd", map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
			"worker": {
				ComponentType: commonconsts.ComponentTypeWorker,
				Envs:          []corev1.EnvVar{{Name: "WORKER_VERSION", Value: "new"}},
			},
		})
		dgd.Annotations = map[string]string{
			commonconsts.AnnotationCurrentWorkerHash: "old-worker-hash",
		}
		reconciler := createTestReconcilerWithStatus(dgd)
		program := &componentProgram{reconciler: reconciler}
		status := dgd.DeepCopy().Status

		require.NoError(t, program.reconcileWorkerRollout(context.Background(), dgd, &status))

		require.NotNil(t, status.RollingUpdate)
		assert.Equal(t, nvidiacomv1beta1.RollingUpdatePhasePending, status.RollingUpdate.Phase)
		assert.Nil(t, dgd.Status.RollingUpdate)
		assert.Equal(t, "old-worker-hash", dgd.Annotations[commonconsts.AnnotationCurrentWorkerHash])
	})

	t.Run("multinode component workload keeps unsupported-path hash behavior", func(t *testing.T) {
		dgd := createTestDGD("test-dgd", map[string]*nvidiacomv1alpha1.DynamoComponentDeploymentSharedSpec{
			"worker": {
				ComponentType: commonconsts.ComponentTypeWorker,
				Envs:          []corev1.EnvVar{{Name: "WORKER_VERSION", Value: "new"}},
				Multinode:     &nvidiacomv1alpha1.MultinodeSpec{NodeCount: 2},
			},
		})
		dgd.Annotations = map[string]string{
			commonconsts.AnnotationCurrentWorkerHash: "old-worker-hash",
		}
		reconciler := createTestReconcilerWithStatus(dgd)
		program := &componentProgram{reconciler: reconciler}
		status := dgd.DeepCopy().Status

		require.NoError(t, program.reconcileWorkerRollout(context.Background(), dgd, &status))

		assert.Nil(t, status.RollingUpdate)
		assert.Nil(t, dgd.Status.RollingUpdate)
		desired, err := reconciler.desiredWorkerHashes(dgd)
		require.NoError(t, err)
		assert.True(t, currentWorkerHashesMatchDesired(reconciler.currentWorkerHashes(dgd), desired))
	})
}

func TestComponentProgram_PreserveExistingBackendFramework(t *testing.T) {
	ctx := context.Background()
	existing := &nvidiacomv1beta1.DynamoComponentDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vllm-disagg-planner-frontend",
			Namespace: "jsm",
		},
		Spec: nvidiacomv1beta1.DynamoComponentDeploymentSpec{
			BackendFramework: "",
			DynamoComponentDeploymentSharedSpec: nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
				ComponentName: "Frontend",
				ComponentType: nvidiacomv1beta1.ComponentTypeFrontend,
			},
		},
	}
	program := &componentProgram{
		reconciler: &DynamoGraphDeploymentReconciler{
			Client: fake.NewClientBuilder().
				WithScheme(newDynamoGraphDeploymentControllerTestScheme(t)).
				WithObjects(existing).
				Build(),
		},
	}

	t.Log("Preserve the immutable stored backend when updating an existing DCD")
	desiredExisting := &nvidiacomv1beta1.DynamoComponentDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      existing.Name,
			Namespace: existing.Namespace,
		},
		Spec: nvidiacomv1beta1.DynamoComponentDeploymentSpec{
			BackendFramework: "vllm",
		},
	}
	require.NoError(t, program.preserveExistingBackendFramework(ctx, desiredExisting))
	assert.Empty(t, desiredExisting.Spec.BackendFramework)

	t.Log("Keep the inferred backend when creating a new DCD")
	desiredNew := &nvidiacomv1beta1.DynamoComponentDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "vllm-disagg-planner-vllmdecodeworker-2dad72b9",
			Namespace: "jsm",
		},
		Spec: nvidiacomv1beta1.DynamoComponentDeploymentSpec{
			BackendFramework: "vllm",
		},
	}
	require.NoError(t, program.preserveExistingBackendFramework(ctx, desiredNew))
	assert.Equal(t, "vllm", desiredNew.Spec.BackendFramework)
}

func TestComponentProgram_ApplyCheckpointStartupPolicy(t *testing.T) {
	program := &componentProgram{}

	t.Run("immediate stamps stable restore candidate metadata", func(t *testing.T) {
		dcd := &nvidiacomv1beta1.DynamoComponentDeployment{
			Spec: nvidiacomv1beta1.DynamoComponentDeploymentSpec{
				DynamoComponentDeploymentSharedSpec: nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
					Replicas: ptr.To(int32(2)),
					PodTemplate: &corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Labels: map[string]string{
								snapshotprotocol.CheckpointIDLabel: "stale",
							},
							Annotations: map[string]string{
								snapshotprotocol.CheckpointStatusAnnotation: "stale",
							},
						},
					},
				},
			},
		}
		info := &checkpoint.CheckpointInfo{
			Enabled:        true,
			Exists:         true,
			Ready:          true,
			Hash:           "checkpoint-id",
			CheckpointName: "checkpoint-name",
			StartupPolicy:  nvidiacomv1alpha1.CheckpointStartupPolicyImmediate,
		}

		require.NoError(t, program.applyCheckpointStartupPolicy(dcd, info))

		require.NotNil(t, dcd.Spec.Experimental)
		require.NotNil(t, dcd.Spec.Experimental.Checkpoint)
		require.NotNil(t, dcd.Spec.Experimental.Checkpoint.CheckpointRef)
		assert.Equal(t, "checkpoint-name", *dcd.Spec.Experimental.Checkpoint.CheckpointRef)
		assert.Nil(t, dcd.Spec.Experimental.Checkpoint.Identity)
		assert.Nil(t, dcd.Spec.Experimental.Checkpoint.Job)
		assert.Equal(t, nvidiacomv1beta1.CheckpointStartupPolicyImmediate, dcd.Spec.Experimental.Checkpoint.StartupPolicy)
		assert.Equal(t, int32(2), *dcd.Spec.Replicas)
		assert.Empty(t, dcd.Spec.PodTemplate.Labels[snapshotprotocol.CheckpointIDLabel])
		assert.Equal(t, commonconsts.KubeLabelValueTrue, dcd.Spec.PodTemplate.Annotations[commonconsts.CheckpointRestoreCandidateAnnotation])
		assert.Equal(t, "checkpoint-name", dcd.Spec.PodTemplate.Annotations[commonconsts.CheckpointNameAnnotation])
		assert.Equal(t, commonconsts.MainContainerName, dcd.Spec.PodTemplate.Annotations[snapshotprotocol.TargetContainersAnnotation])
	})

	t.Run("wait for checkpoint gates replicas until ready", func(t *testing.T) {
		dcd := &nvidiacomv1beta1.DynamoComponentDeployment{
			Spec: nvidiacomv1beta1.DynamoComponentDeploymentSpec{
				DynamoComponentDeploymentSharedSpec: nvidiacomv1beta1.DynamoComponentDeploymentSharedSpec{
					Replicas: ptr.To(int32(3)),
				},
			},
		}
		info := &checkpoint.CheckpointInfo{
			Enabled:        true,
			Exists:         true,
			Ready:          false,
			CheckpointName: "checkpoint-name",
			StartupPolicy:  nvidiacomv1alpha1.CheckpointStartupPolicyWaitForCheckpoint,
		}

		require.NoError(t, program.applyCheckpointStartupPolicy(dcd, info))

		require.NotNil(t, dcd.Spec.Experimental)
		require.NotNil(t, dcd.Spec.Experimental.Checkpoint)
		require.NotNil(t, dcd.Spec.Experimental.Checkpoint.CheckpointRef)
		assert.Equal(t, "checkpoint-name", *dcd.Spec.Experimental.Checkpoint.CheckpointRef)
		assert.Equal(t, nvidiacomv1beta1.CheckpointStartupPolicyWaitForCheckpoint, dcd.Spec.Experimental.Checkpoint.StartupPolicy)
		assert.Equal(t, int32(0), *dcd.Spec.Replicas)
	})
}
