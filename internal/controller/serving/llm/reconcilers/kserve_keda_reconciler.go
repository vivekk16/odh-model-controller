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
	"fmt"

	"github.com/go-logr/logr"
	kservev1alpha2 "github.com/kserve/kserve/pkg/apis/serving/v1alpha2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	parentreconcilers "github.com/opendatahub-io/odh-model-controller/internal/controller/serving/reconcilers"
)

var _ parentreconcilers.LLMSubResourceReconciler = (*KserveKEDAReconciler)(nil)

// KserveKEDAReconciler allows LLMInferenceServices using standalone (direct) KEDA scaling with
// a "prometheus" trigger to authenticate to OpenShift Monitoring (Thanos/Prometheus).
//
// This is a thin, LLMInferenceService-specific wrapper around
// parentreconcilers.KEDAPrometheusAuthReconciler, the same owner-agnostic engine used by
// InferenceService's KEDA reconciler (internal/controller/serving/reconcilers). Both CRDs
// share the exact same underlying per-namespace ServiceAccount/Secret/Role/RoleBinding/
// TriggerAuthentication objects (multi-owner) rather than each getting a duplicate set - see
// KEDAPrometheusAuthReconciler's doc comment for why.
//
// WVA-mediated KEDA (spec.scaling.wva.keda) is out of scope for this reconciler: it already
// authenticates via a separate, cluster-scoped ClusterTriggerAuthentication configured through
// kserve's inferenceservice-config ConfigMap.
type KserveKEDAReconciler struct {
	engine *parentreconcilers.KEDAPrometheusAuthReconciler
}

func NewKServeKEDAReconciler(client client.Client) *KserveKEDAReconciler {
	return &KserveKEDAReconciler{
		engine: parentreconcilers.NewKEDAPrometheusAuthReconciler(client),
	}
}

func (k *KserveKEDAReconciler) Reconcile(ctx context.Context, log logr.Logger, llmisvc *kservev1alpha2.LLMInferenceService) error {
	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("Reconciling LLMInferenceService", "LLMInferenceService", llmisvc)

	if !parentreconcilers.HasPrometheusKEDATrigger(llmisvc) {
		log.V(1).Info("No Prometheus KEDA trigger found, KEDA auth resources not required by this LLMInferenceService. Ensuring LLMInferenceService is removed from owner references.")

		if err := k.engine.RemoveOwnerReference(ctx, log, llmisvc.GetNamespace(), asLLMIsvcOwnerRef(llmisvc)); err != nil {
			return fmt.Errorf("failed to remove owner reference from KEDA resources: %w", err)
		}
		return k.engine.MaybeCleanupNamespace(ctx, log, llmisvc.GetNamespace())
	}

	log.Info("Reconciling resources")

	if err := k.engine.EnsureResources(ctx, log, llmisvc.GetNamespace(), asLLMIsvcOwnerRef(llmisvc), llmisvc.GetAnnotations()); err != nil {
		return err
	}
	log.Info("Successfully reconciled KEDA resources")
	return nil
}

func (k *KserveKEDAReconciler) Delete(ctx context.Context, log logr.Logger, llmisvc *kservev1alpha2.LLMInferenceService) error {
	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("KserveKEDAReconciler.Delete called")

	if err := k.engine.RemoveOwnerReference(ctx, log, llmisvc.GetNamespace(), asLLMIsvcOwnerRef(llmisvc)); err != nil {
		return fmt.Errorf("failed to remove owner reference from KEDA resources: %w", err)
	}
	return k.engine.MaybeCleanupNamespace(ctx, log, llmisvc.GetNamespace())
}

func (k *KserveKEDAReconciler) Cleanup(ctx context.Context, log logr.Logger, llmisvcNs string) error {
	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("KserveKEDAReconciler.Cleanup called.", "namespace", llmisvcNs)
	// MaybeCleanupNamespace re-checks both InferenceServices and LLMInferenceServices in the
	// namespace before deleting anything, so it's safe to call even though this caller
	// (DeleteResourcesIfNoLLMIsvcExists) only knows about LLMInferenceServices.
	return k.engine.MaybeCleanupNamespace(ctx, log, llmisvcNs)
}

func asLLMIsvcOwnerRef(llmisvc *kservev1alpha2.LLMInferenceService) metav1.OwnerReference {
	return parentreconcilers.AsOwnerRef(llmisvc, kservev1alpha2.LLMInferenceServiceGVK)
}
