/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package reconcilers

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kservev1alpha2 "github.com/kserve/kserve/pkg/apis/serving/v1alpha2"
	kservev1beta1 "github.com/kserve/kserve/pkg/apis/serving/v1beta1"
	"github.com/opendatahub-io/odh-model-controller/internal/controller/utils"
)

func TestHasPrometheusKEDATrigger(t *testing.T) {
	prometheusTrigger := kedav1alpha1.ScaleTriggers{Type: "prometheus", Metadata: map[string]string{"serverAddress": "http://prom:9090", "query": "up"}}
	cpuTrigger := kedav1alpha1.ScaleTriggers{Type: "cpu", Metadata: map[string]string{"value": "80"}}

	tests := []struct {
		name    string
		llmisvc *kservev1alpha2.LLMInferenceService
		want    bool
	}{
		{
			name:    "no scaling configured",
			llmisvc: &kservev1alpha2.LLMInferenceService{},
			want:    false,
		},
		{
			name: "main scaling with direct KEDA prometheus trigger",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					WorkloadSpec: kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 3,
							KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
								Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "main scaling with direct KEDA non-prometheus trigger only",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					WorkloadSpec: kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 3,
							KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
								Triggers: []kedav1alpha1.ScaleTriggers{cpuTrigger},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "main scaling with WVA-mediated KEDA is excluded",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					WorkloadSpec: kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 3,
							WVA: &kservev1alpha2.WVASpec{
								ActuatorSpec: kservev1alpha2.ActuatorSpec{
									KEDA: &kservev1alpha2.KEDAScalingSpec{},
								},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "main scaling with HPA only",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					WorkloadSpec: kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 3,
							WVA: &kservev1alpha2.WVASpec{
								ActuatorSpec: kservev1alpha2.ActuatorSpec{
									HPA: &kservev1alpha2.HPAScalingSpec{},
								},
							},
						},
					},
				},
			},
			want: false,
		},
		{
			name: "prefill scaling with direct KEDA prometheus trigger, main unset",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					Prefill: &kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 2,
							KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
								Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "main non-prometheus, prefill prometheus - still true",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					WorkloadSpec: kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 3,
							KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
								Triggers: []kedav1alpha1.ScaleTriggers{cpuTrigger},
							},
						},
					},
					Prefill: &kservev1alpha2.WorkloadSpec{
						Scaling: &kservev1alpha2.ScalingSpec{
							MaxReplicas: 2,
							KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
								Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
							},
						},
					},
				},
			},
			want: true,
		},
		{
			name: "prefill present but scaling nil",
			llmisvc: &kservev1alpha2.LLMInferenceService{
				Spec: kservev1alpha2.LLMInferenceServiceSpec{
					Prefill: &kservev1alpha2.WorkloadSpec{},
				},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, HasPrometheusKEDATrigger(tt.llmisvc))
		})
	}
}

func TestAsOwnerRefForLLMInferenceService(t *testing.T) {
	llmisvc := &kservev1alpha2.LLMInferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-llm",
			UID:  "abc-123",
		},
	}

	ref := AsOwnerRef(llmisvc, kservev1alpha2.LLMInferenceServiceGVK)

	assert.Equal(t, "my-llm", ref.Name)
	assert.Equal(t, "LLMInferenceService", ref.Kind)
	assert.NotNil(t, ref.Controller)
	assert.False(t, *ref.Controller, "owner reference must be non-controlling so it can coexist with an InferenceService owner")
}

// TestKEDAPrometheusAuthReconciler_CrossKindSharingAndCleanup verifies that InferenceService and
// LLMInferenceService share the exact same per-namespace KEDA Prometheus-auth objects (rather
// than each getting a duplicate set), and that those shared objects are only deleted once
// neither kind needs them anymore - the core correctness fix for reusing this reconciler across
// both CRDs.
func TestKEDAPrometheusAuthReconciler_CrossKindSharingAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	utils.RegisterSchemes(scheme)

	const namespace = "test-ns"

	isvc := &kservev1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-isvc", Namespace: namespace, UID: "isvc-uid"},
	}
	llmisvc := &kservev1alpha2.LLMInferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "my-llmisvc", Namespace: namespace, UID: "llmisvc-uid"},
		Spec: kservev1alpha2.LLMInferenceServiceSpec{
			WorkloadSpec: kservev1alpha2.WorkloadSpec{
				Scaling: &kservev1alpha2.ScalingSpec{
					MaxReplicas: 3,
					KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
						Triggers: []kedav1alpha1.ScaleTriggers{{Type: "prometheus", Metadata: map[string]string{"query": "up"}}},
					},
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(isvc, llmisvc).Build()

	engine := NewKEDAPrometheusAuthReconciler(fakeClient)
	logger := logr.Discard()
	ctx := context.Background()

	isvcOwnerRef := AsIsvcOwnerRef(isvc)
	llmisvcOwnerRef := AsOwnerRef(llmisvc, kservev1alpha2.LLMInferenceServiceGVK)

	require.NoError(t, engine.EnsureResources(ctx, logger, namespace, isvcOwnerRef, isvc.Annotations))
	require.NoError(t, engine.EnsureResources(ctx, logger, namespace, llmisvcOwnerRef, llmisvc.Annotations))

	sa := &corev1.ServiceAccount{}
	require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: KEDAPrometheusAuthServiceAccountName}, sa))
	assert.Len(t, sa.OwnerReferences, 2, "shared ServiceAccount should have both InferenceService and LLMInferenceService as owners")

	// Removing the InferenceService's ownership must not delete the shared resources while the
	// LLMInferenceService (still present, still needing a Prometheus KEDA trigger) is around.
	require.NoError(t, engine.RemoveOwnerReference(ctx, logger, namespace, isvcOwnerRef))
	require.NoError(t, engine.MaybeCleanupNamespace(ctx, logger, namespace))

	require.NoError(t, fakeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: KEDAPrometheusAuthServiceAccountName}, sa))
	require.Len(t, sa.OwnerReferences, 1)
	assert.Equal(t, llmisvcOwnerRef.UID, sa.OwnerReferences[0].UID)

	// Now remove the LLMInferenceService too (simulating deletion), and confirm the shared
	// resources are only cleaned up once nothing of either kind needs them anymore.
	require.NoError(t, fakeClient.Delete(ctx, llmisvc))
	require.NoError(t, engine.RemoveOwnerReference(ctx, logger, namespace, llmisvcOwnerRef))
	require.NoError(t, engine.MaybeCleanupNamespace(ctx, logger, namespace))

	err := fakeClient.Get(ctx, client.ObjectKey{Namespace: namespace, Name: KEDAPrometheusAuthServiceAccountName}, sa)
	assert.True(t, apierrors.IsNotFound(err), "shared ServiceAccount should be deleted once neither InferenceService nor LLMInferenceService needs it")
}
