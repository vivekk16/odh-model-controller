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

package llm_test

import (
	"context"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	kservev1alpha2 "github.com/kserve/kserve/pkg/apis/serving/v1alpha2"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/opendatahub-io/odh-model-controller/internal/controller/serving/llm/fixture"
	parentreconcilers "github.com/opendatahub-io/odh-model-controller/internal/controller/serving/reconcilers"
	testutils "github.com/opendatahub-io/odh-model-controller/test/utils"
)

var _ = Describe("LLMInferenceService KEDA Prometheus Auth", func() {
	var testNs string

	BeforeEach(func() {
		ctx := context.Background()
		testNamespace := testutils.Namespaces.Create(ctx, envTest.Client)
		testNs = testNamespace.Name
	})

	AfterEach(func(ctx SpecContext) {
		llmList := &kservev1alpha2.LLMInferenceServiceList{}
		if err := envTest.Client.List(ctx, llmList, client.InNamespace(testNs)); err == nil {
			for i := range llmList.Items {
				_ = envTest.Client.Delete(ctx, &llmList.Items[i])
			}
		}
	})

	prometheusTrigger := kedav1alpha1.ScaleTriggers{
		Type:     "prometheus",
		Metadata: map[string]string{"serverAddress": "http://prom:9090", "query": "up", "threshold": "1"},
	}
	cpuTrigger := kedav1alpha1.ScaleTriggers{
		Type:     "cpu",
		Metadata: map[string]string{"value": "80"},
	}

	assertKedaAuthResourcesExist := func(ctx context.Context, ownerUID types.UID) {
		sa := &corev1.ServiceAccount{}
		Eventually(func(g Gomega) {
			g.Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthServiceAccountName, Namespace: testNs}, sa)).To(Succeed())
			g.Expect(sa).To(HaveOwnerReferenceByUID(ownerUID))
		}).WithContext(ctx).Should(Succeed())

		secret := &corev1.Secret{}
		Eventually(func(g Gomega) {
			g.Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthTriggerSecretName, Namespace: testNs}, secret)).To(Succeed())
			g.Expect(secret).To(HaveOwnerReferenceByUID(ownerUID))
		}).WithContext(ctx).Should(Succeed())

		role := &rbacv1.Role{}
		Eventually(func(g Gomega) {
			g.Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthMetricsReaderRoleName, Namespace: testNs}, role)).To(Succeed())
			g.Expect(role).To(HaveOwnerReferenceByUID(ownerUID))
		}).WithContext(ctx).Should(Succeed())

		roleBinding := &rbacv1.RoleBinding{}
		Eventually(func(g Gomega) {
			g.Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthMetricsReaderRoleBindingName, Namespace: testNs}, roleBinding)).To(Succeed())
			g.Expect(roleBinding).To(HaveOwnerReferenceByUID(ownerUID))
		}).WithContext(ctx).Should(Succeed())

		ta := &kedav1alpha1.TriggerAuthentication{}
		Eventually(func(g Gomega) {
			g.Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthTriggerAuthName, Namespace: testNs}, ta)).To(Succeed())
			g.Expect(ta).To(HaveOwnerReferenceByUID(ownerUID))
		}).WithContext(ctx).Should(Succeed())
	}

	assertKedaAuthResourcesDoNotExist := func(ctx context.Context) {
		Consistently(func() bool {
			sa := &corev1.ServiceAccount{}
			err := envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthServiceAccountName, Namespace: testNs}, sa)
			return errors.IsNotFound(err)
		}).WithContext(ctx).Should(BeTrue())
	}

	It("should create KEDA auth resources for a main-workload direct KEDA prometheus trigger", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 3,
				KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
					Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())

		assertKedaAuthResourcesExist(ctx, llmisvc.UID)
	})

	It("should create KEDA auth resources for a prefill-only direct KEDA prometheus trigger", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithPrefillScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 2,
				KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
					Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())

		assertKedaAuthResourcesExist(ctx, llmisvc.UID)
	})

	It("should not create KEDA auth resources for a non-prometheus direct KEDA trigger", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 3,
				KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
					Triggers: []kedav1alpha1.ScaleTriggers{cpuTrigger},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())

		assertKedaAuthResourcesDoNotExist(ctx)
	})

	It("should not create KEDA auth resources for WVA-mediated KEDA scaling", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 3,
				WVA: &kservev1alpha2.WVASpec{
					ActuatorSpec: kservev1alpha2.ActuatorSpec{
						KEDA: &kservev1alpha2.KEDAScalingSpec{},
					},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())

		assertKedaAuthResourcesDoNotExist(ctx)
	})

	It("should clean up KEDA auth resources once the LLMInferenceService is deleted", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 3,
				KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
					Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())
		assertKedaAuthResourcesExist(ctx, llmisvc.UID)

		// envtest runs no garbage collector, and LLMInferenceService gets no finalizer by
		// default, so a plain Delete would remove the object immediately - before our
		// reconciler ever observes a DeletionTimestamp and runs its cleanup logic. Add a
		// temporary finalizer (same pattern used in auth_posture_test.go) to hold the object
		// in a "terminating" state long enough to exercise that codepath.
		Expect(retry.RetryOnConflict(retry.DefaultRetry, func() error {
			Expect(envTest.Client.Get(ctx, client.ObjectKeyFromObject(llmisvc), llmisvc)).To(Succeed())
			llmisvc.Finalizers = []string{"test.opendatahub.io/block-delete"}
			return envTest.Client.Update(ctx, llmisvc)
		})).To(Succeed())

		Expect(envTest.Client.Delete(ctx, llmisvc)).To(Succeed())

		Eventually(func() bool {
			sa := &corev1.ServiceAccount{}
			err := envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthServiceAccountName, Namespace: testNs}, sa)
			return errors.IsNotFound(err)
		}).WithContext(ctx).Should(BeTrue())

		// Remove the finalizer so envtest can fully delete the object.
		if err := envTest.Client.Get(ctx, client.ObjectKeyFromObject(llmisvc), llmisvc); err == nil {
			llmisvc.Finalizers = nil
			_ = envTest.Client.Update(ctx, llmisvc)
		}
	})

	It("should remove KEDA auth resources when scaling is updated to no longer use a prometheus trigger", func(ctx SpecContext) {
		llmisvc := fixture.LLMInferenceService(LLMInferenceServiceName,
			fixture.InNamespace[*kservev1alpha2.LLMInferenceService](testNs),
			fixture.WithScaling(&kservev1alpha2.ScalingSpec{
				MaxReplicas: 3,
				KEDA: &kservev1alpha2.DirectKEDAScalingSpec{
					Triggers: []kedav1alpha1.ScaleTriggers{prometheusTrigger},
				},
			}),
		)
		Expect(envTest.Client.Create(ctx, llmisvc)).To(Succeed())
		assertKedaAuthResourcesExist(ctx, llmisvc.UID)

		latest := &kservev1alpha2.LLMInferenceService{}
		Expect(envTest.Client.Get(ctx, types.NamespacedName{Name: llmisvc.Name, Namespace: testNs}, latest)).To(Succeed())
		latest.Spec.Scaling.KEDA.Triggers = []kedav1alpha1.ScaleTriggers{cpuTrigger}
		Expect(envTest.Client.Update(ctx, latest)).To(Succeed())

		Eventually(func() bool {
			sa := &corev1.ServiceAccount{}
			err := envTest.Client.Get(ctx, types.NamespacedName{Name: parentreconcilers.KEDAPrometheusAuthServiceAccountName, Namespace: testNs}, sa)
			if err != nil {
				return errors.IsNotFound(err)
			}
			for _, ref := range sa.OwnerReferences {
				if ref.UID == llmisvc.UID {
					return false
				}
			}
			return true
		}).WithContext(ctx).Should(BeTrue())
	})
})

// HaveOwnerReferenceByUID succeeds if the object has an owner reference with the given UID.
func HaveOwnerReferenceByUID(uid types.UID) OmegaMatcher {
	return WithTransform(func(obj client.Object) bool {
		for _, ref := range obj.GetOwnerReferences() {
			if ref.UID == uid {
				return true
			}
		}
		return false
	}, BeTrue())
}
