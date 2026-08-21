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
	"github.com/hashicorp/go-multierror"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	kedaapi "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	kservev1alpha2 "github.com/kserve/kserve/pkg/apis/serving/v1alpha2"
	kservev1beta1 "github.com/kserve/kserve/pkg/apis/serving/v1beta1"
)

const (
	KEDAResourcesPrefix                            = "inference-"
	KEDAPrometheusPrefix                           = KEDAResourcesPrefix + "prometheus-"
	KEDAPrometheusAuthResourceName                 = KEDAPrometheusPrefix + "auth"
	KEDAPrometheusAuthServiceAccountName           = KEDAPrometheusAuthResourceName
	KEDAPrometheusAuthTriggerSecretName            = KEDAPrometheusAuthResourceName
	KEDAPrometheusAuthMetricsReaderRoleName        = KEDAPrometheusAuthResourceName
	KEDAPrometheusAuthMetricsReaderRoleBindingName = KEDAPrometheusAuthResourceName
	KEDAPrometheusAuthTriggerAuthName              = KEDAPrometheusAuthResourceName

	KEDAResourcesLabelKey   = "odh-model-controller"
	KEDAResourcesLabelValue = "keda-reconciler"
)

var KedaLabelPredicate = predicate.NewPredicateFuncs(func(o client.Object) bool {
	return o.GetLabels()[KEDAResourcesLabelKey] == KEDAResourcesLabelValue
})

var _ SubResourceReconciler = (*KserveKEDAReconciler)(nil)

// KserveKEDAReconciler allows ISVCs to autoscale on custom Prometheus metrics via KEDA, with secure OpenShift Monitoring
// access. The reconciler automates KEDA/RBAC resource lifecycle.
//
// This is a thin, InferenceService-specific wrapper around KEDAPrometheusAuthReconciler, the
// owner-agnostic engine that actually manages the shared per-namespace auth resources. The
// same engine (and the same underlying Kubernetes objects) is also used by LLMInferenceService's
// KEDA reconciler (internal/controller/serving/llm/reconcilers) - see KEDAPrometheusAuthReconciler
// for details on why these resources are namespace-singleton and multi-owner rather than
// duplicated per CRD kind.
//
// - Creates resources if InferenceService uses KEDA Prometheus external metric.
// - Adds each InferenceService in a given namespace as non-controlling owner to shared namespaced resources.
// - Removes InferenceService owner reference if KEDA Prometheus autoscaling is unused or InferenceService deleted.
// - Cleans up KEDA resources from namespace if no InferenceServices *or* LLMInferenceServices use KEDA Prometheus autoscaling.
type KserveKEDAReconciler struct {
	engine *KEDAPrometheusAuthReconciler
}

func NewKServeKEDAReconciler(client client.Client) *KserveKEDAReconciler {
	return &KserveKEDAReconciler{
		engine: NewKEDAPrometheusAuthReconciler(client),
	}
}

func (k *KserveKEDAReconciler) Reconcile(ctx context.Context, log logr.Logger, isvc *kservev1beta1.InferenceService) error {

	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("Reconciling InferenceService", "InferenceService", isvc)

	if !hasPrometheusExternalAutoscalingMetric(isvc, log) {
		log.V(1).Info("No Prometheus external autoscaling metric found, KEDA resources not required by this InferenceService. Ensuring InferenceService is removed from owner references.")

		// When Prometheus autoscaling is not used, we remove the InferenceService as owner reference from
		// all KEDA-related resources.
		if err := k.engine.RemoveOwnerReference(ctx, log, isvc.GetNamespace(), AsIsvcOwnerRef(isvc)); err != nil {
			return fmt.Errorf("failed to remove owner reference from KEDA resources: %w", err)
		}
		return k.engine.MaybeCleanupNamespace(ctx, log, isvc.GetNamespace())
	}

	log.Info("Reconciling resources")

	if err := k.engine.EnsureResources(ctx, log, isvc.GetNamespace(), AsIsvcOwnerRef(isvc), isvc.Annotations); err != nil {
		return err
	}
	log.Info("Successfully reconciled KEDA resources")
	return nil
}

func (k *KserveKEDAReconciler) Delete(ctx context.Context, log logr.Logger, isvc *kservev1beta1.InferenceService) error {

	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("KserveKEDAReconciler.Delete called")

	if err := k.engine.RemoveOwnerReference(ctx, log, isvc.GetNamespace(), AsIsvcOwnerRef(isvc)); err != nil {
		return fmt.Errorf("failed to remove owner reference from KEDA resources: %w", err)
	}
	return k.engine.MaybeCleanupNamespace(ctx, log, isvc.GetNamespace())
}

func (k *KserveKEDAReconciler) Cleanup(ctx context.Context, log logr.Logger, isvcNs string) error {
	log = log.WithName("KserveKEDAReconciler")
	log.V(2).Info("KserveKEDAReconciler.Cleanup called.", "namespace", isvcNs)
	// Re-verify (rather than trust the caller) that neither an InferenceService nor an
	// LLMInferenceService in this namespace still needs these shared resources. Callers of
	// Cleanup only know about their own CRD kind (see DeleteResourcesIfNoIsvcExists /
	// DeleteResourcesIfNoLLMIsvcExists), so this reconciler must be the one place that checks both.
	return k.engine.MaybeCleanupNamespace(ctx, log, isvcNs)
}

// hasPrometheusExternalAutoscalingMetric returns true if the InferenceService's predictor
// declares a KEDA "prometheus" external autoscaling metric.
func hasPrometheusExternalAutoscalingMetric(isvc *kservev1beta1.InferenceService, log logr.Logger) bool {
	log.V(1).Info("hasPrometheusExternalAutoscalingMetric", "autoscaling", isvc.Spec.Predictor.AutoScaling)
	if isvc.Spec.Predictor.AutoScaling == nil {
		return false
	}
	for _, m := range isvc.Spec.Predictor.AutoScaling.Metrics {
		if m.External != nil && m.External.Metric.Backend == kservev1beta1.PrometheusBackend {
			return true
		}
	}
	return false
}

// HasPrometheusKEDATrigger returns true if the LLMInferenceService's standalone (direct) KEDA
// scaling configuration - main or prefill - includes a user-defined trigger backed by the
// "prometheus" scaler.
//
// WVA-mediated KEDA (spec.scaling.wva.keda / spec.prefill.scaling.wva.keda) is intentionally
// excluded here: it already authenticates to Prometheus/Thanos via a separate, cluster-scoped
// ClusterTriggerAuthentication configured through kserve's inferenceservice-config ConfigMap
// (autoscaling-wva-controller-config key), and does not need the namespaced auth resources
// this reconciler manages.
func HasPrometheusKEDATrigger(llmisvc *kservev1alpha2.LLMInferenceService) bool {
	if scalingSpecHasPrometheusKEDATrigger(llmisvc.Spec.Scaling) {
		return true
	}
	if llmisvc.Spec.Prefill != nil && scalingSpecHasPrometheusKEDATrigger(llmisvc.Spec.Prefill.Scaling) {
		return true
	}
	return false
}

func scalingSpecHasPrometheusKEDATrigger(scaling *kservev1alpha2.ScalingSpec) bool {
	if scaling == nil || scaling.KEDA == nil {
		return false
	}
	for _, trigger := range scaling.KEDA.Triggers {
		if trigger.Type == "prometheus" {
			return true
		}
	}
	return false
}

// KEDAPrometheusAuthReconciler manages KEDA-specific resources (ServiceAccount, Secret, Role,
// RoleBinding, TriggerAuthentication) that let a KEDA "prometheus" trigger authenticate to
// OpenShift's Thanos/Prometheus. It is namespace-scoped and owner-agnostic: any object kind
// that needs authenticated Prometheus access from a KEDA ScaledObject (currently InferenceService
// and LLMInferenceService) can call into it, and all such consumers in a namespace share the
// exact same underlying objects via non-controlling, multi-owner OwnerReferences rather than
// each getting a duplicate set.
type KEDAPrometheusAuthReconciler struct {
	client client.Client
}

func NewKEDAPrometheusAuthReconciler(client client.Client) *KEDAPrometheusAuthReconciler {
	return &KEDAPrometheusAuthReconciler{
		client: client,
	}
}

// EnsureResources creates or updates the full set of shared KEDA Prometheus-auth resources in
// namespace, adding ownerRef as a (non-controlling) owner of each.
func (k *KEDAPrometheusAuthReconciler) EnsureResources(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	if err := retryOnConflicts(func() error { return k.reconcileServiceAccount(ctx, log, namespace, ownerRef) }); err != nil {
		return fmt.Errorf("failed to reconcile service account: %w", err)
	}
	if err := retryOnConflicts(func() error { return k.reconcileSecret(ctx, log, namespace, ownerRef) }); err != nil {
		return fmt.Errorf("failed to reconcile secret: %w", err)
	}
	if err := retryOnConflicts(func() error { return k.reconcileRole(ctx, log, namespace, ownerRef, annotations) }); err != nil {
		return fmt.Errorf("failed to reconcile role: %w", err)
	}
	if err := retryOnConflicts(func() error { return k.reconcileRoleBinding(ctx, log, namespace, ownerRef, annotations) }); err != nil {
		return fmt.Errorf("failed to reconcile role binding: %w", err)
	}
	if err := retryOnConflicts(func() error { return k.reconcileTriggerAuthentication(ctx, log, namespace, ownerRef, annotations) }); err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("failed to reconcile trigger authentication: %w", err)
	}
	return nil
}

// RemoveOwnerReference removes ownerRef from all shared KEDA Prometheus-auth resources in
// namespace, without deleting the resources themselves. Callers should follow this with
// MaybeCleanupNamespace to delete the resources once nothing owns them anymore.
func (k *KEDAPrometheusAuthReconciler) RemoveOwnerReference(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference) error {
	var encounteredErrors []error

	for _, rc := range k.resourcesToCleanup() {
		if err := retryOnConflicts(func() error { return k.removeOwnerReferenceFromObject(ctx, log, rc, namespace, ownerRef) }); err != nil {
			err := fmt.Errorf("failed to remove owner reference from %s %s: %w", rc.GetObjectKind(), rc.GetName(), err)
			encounteredErrors = append(encounteredErrors, err)
		}
	}

	if len(encounteredErrors) > 0 {
		err := multierror.Append(nil, encounteredErrors...)
		return fmt.Errorf("encountered %d error(s) during owner reference removal: %w", len(encounteredErrors), err)
	}
	return nil
}

// MaybeCleanupNamespace deletes the shared KEDA Prometheus-auth resources in namespace, but
// only if no InferenceService and no LLMInferenceService in that namespace still needs them.
// This check is deliberately performed here (rather than trusted from callers) because callers
// such as DeleteResourcesIfNoIsvcExists / DeleteResourcesIfNoLLMIsvcExists only have visibility
// into their own CRD kind.
//
// Listing either kind tolerates that kind's CRD not being installed at all (meta.IsNoMatchError):
// a deployment running only the LLM controller (or only the v1beta1 controller) without the other
// CRD installed is treated as having zero instances of that kind, not as an error.
//
// Objects that are themselves terminating (non-zero DeletionTimestamp, e.g. held open by their
// own finalizer) are not counted as "still needing" the shared resources - otherwise the very
// object whose deletion triggered this call would block its own cleanup.
func (k *KEDAPrometheusAuthReconciler) MaybeCleanupNamespace(ctx context.Context, log logr.Logger, namespace string) error {
	inferenceServiceList := &kservev1beta1.InferenceServiceList{}
	if err := k.client.List(ctx, inferenceServiceList, client.InNamespace(namespace)); err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("failed to list InferenceServices in namespace %s: %w", namespace, err)
	}
	for _, isvc := range inferenceServiceList.Items {
		if isvc.GetDeletionTimestamp().IsZero() && hasPrometheusExternalAutoscalingMetric(&isvc, log) {
			// There are still InferenceServices with Prometheus autoscaling configured, nothing to remove.
			return nil
		}
	}

	llmInferenceServiceList := &kservev1alpha2.LLMInferenceServiceList{}
	if err := k.client.List(ctx, llmInferenceServiceList, client.InNamespace(namespace)); err != nil && !meta.IsNoMatchError(err) {
		return fmt.Errorf("failed to list LLMInferenceServices in namespace %s: %w", namespace, err)
	}
	for _, llmisvc := range llmInferenceServiceList.Items {
		if llmisvc.GetDeletionTimestamp().IsZero() && HasPrometheusKEDATrigger(&llmisvc) {
			// There are still LLMInferenceServices with a Prometheus KEDA trigger configured, nothing to remove.
			return nil
		}
	}

	return k.cleanupNamespace(ctx, log, namespace)
}

func (k *KEDAPrometheusAuthReconciler) cleanupNamespace(ctx context.Context, log logr.Logger, namespace string) error {
	log.Info("Cleaning up KEDA resources in namespace", "namespace", namespace)
	var encounteredErrors []error
	for _, r := range k.resourcesToCleanup() {
		r.SetNamespace(namespace)
		if err := k.client.Delete(ctx, r); err != nil && !errors.IsNotFound(err) && !meta.IsNoMatchError(err) {
			encounteredErrors = append(encounteredErrors, err)
		}
	}
	if len(encounteredErrors) > 0 {
		return multierror.Append(nil, encounteredErrors...)
	}
	return nil
}

// --- TriggerAuthentication ---
func (k *KEDAPrometheusAuthReconciler) reconcileTriggerAuthentication(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	expected := k.expectedTriggerAuthentication(namespace, ownerRef, annotations)
	curr := &kedaapi.TriggerAuthentication{}
	key := client.ObjectKey{Namespace: expected.Namespace, Name: expected.Name}

	err := k.client.Get(ctx, key, curr)
	if err != nil {
		if errors.IsNotFound(err) {
			return k.createTriggerAuthentication(ctx, log, namespace, ownerRef, annotations)
		}
		return fmt.Errorf("failed to get TriggerAuthentication %s: %w", key.String(), err)
	}

	expected.OwnerReferences = upsertOwnerReference(ownerRef, curr)
	expected.ResourceVersion = curr.ResourceVersion

	if equality.Semantic.DeepDerivative(expected.Spec, curr.Spec) &&
		equality.Semantic.DeepDerivative(expected.Labels, curr.Labels) &&
		equality.Semantic.DeepDerivative(expected.Annotations, curr.Annotations) &&
		equality.Semantic.DeepDerivative(expected.OwnerReferences, curr.OwnerReferences) {
		log.V(2).Info("TriggerAuthentication is up-to-date", "namespace", key.Namespace, "name", key.Name)
		return nil
	}

	log.Info("Updating TriggerAuthentication", "namespace", key.Namespace, "name", key.Name)
	if err := k.client.Update(ctx, expected); err != nil {
		return fmt.Errorf("failed to update TriggerAuthentication %s: %w", key.String(), err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) createTriggerAuthentication(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	log.Info("Creating TriggerAuthentication", "name", KEDAPrometheusAuthTriggerAuthName)
	ta := k.expectedTriggerAuthentication(namespace, ownerRef, annotations)
	if err := k.client.Create(ctx, ta); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create TriggerAuthentication %s/%s: %w", ta.Namespace, ta.Name, err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) expectedTriggerAuthentication(namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) *kedaapi.TriggerAuthentication {
	return &kedaapi.TriggerAuthentication{
		ObjectMeta: metav1.ObjectMeta{
			Name:            KEDAPrometheusAuthTriggerAuthName,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels:          getKedaLabels(),
			Annotations:     annotations,
		},
		Spec: kedaapi.TriggerAuthenticationSpec{
			SecretTargetRef: []kedaapi.AuthSecretTargetRef{
				{
					Parameter: "bearerToken",
					Name:      KEDAPrometheusAuthTriggerSecretName,
					Key:       "token",
				},
				{
					Parameter: "ca",
					Name:      KEDAPrometheusAuthTriggerSecretName,
					Key:       "ca.crt",
				},
			},
		},
	}
}

// --- ServiceAccount ---
func (k *KEDAPrometheusAuthReconciler) reconcileServiceAccount(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference) error {
	expected := k.expectedServiceAccount(namespace, ownerRef)
	curr := &corev1.ServiceAccount{}
	key := client.ObjectKey{Namespace: expected.Namespace, Name: expected.Name}

	err := k.client.Get(ctx, key, curr)
	if err != nil {
		if errors.IsNotFound(err) {
			return k.createServiceAccount(ctx, log, namespace, ownerRef)
		}
		return fmt.Errorf("failed to get ServiceAccount %s: %w", key.String(), err)
	}

	expected.OwnerReferences = upsertOwnerReference(ownerRef, curr)
	expected.ResourceVersion = curr.ResourceVersion

	if equality.Semantic.DeepDerivative(expected.Labels, curr.Labels) &&
		equality.Semantic.DeepDerivative(expected.Annotations, curr.Annotations) &&
		equality.Semantic.DeepDerivative(expected.OwnerReferences, curr.OwnerReferences) {
		log.V(2).Info("ServiceAccount is up-to-date", "namespace", expected.Namespace, "name", expected.Name)
		return nil
	}

	log.Info("Updating ServiceAccount", "namespace", key.Namespace, "name", key.Name)
	if err := k.client.Update(ctx, expected); err != nil {
		return fmt.Errorf("failed to update ServiceAccount %s: %w", key.String(), err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) createServiceAccount(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference) error {
	log.Info("Creating ServiceAccount", "name", KEDAPrometheusAuthServiceAccountName)
	sa := k.expectedServiceAccount(namespace, ownerRef)
	if err := k.client.Create(ctx, sa); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create ServiceAccount %s/%s: %w", sa.Namespace, sa.Name, err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) expectedServiceAccount(namespace string, ownerRef metav1.OwnerReference) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:            KEDAPrometheusAuthServiceAccountName,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels:          getKedaLabels(),
		},
		// ServiceAccount Spec is mostly empty; Secrets and ImagePullSecrets are managed via sub-resources or user additions.
	}
}

// --- Secret ---
func (k *KEDAPrometheusAuthReconciler) reconcileSecret(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference) error {
	expected := k.expectedSecret(namespace, ownerRef)
	curr := &corev1.Secret{}
	key := client.ObjectKey{Namespace: expected.Namespace, Name: expected.Name}

	err := k.client.Get(ctx, key, curr)
	if err != nil {
		if errors.IsNotFound(err) {
			return k.createSecret(ctx, log, namespace, ownerRef)
		}
		return fmt.Errorf("failed to get Secret %s: %w", key.String(), err)
	}

	expected.OwnerReferences = upsertOwnerReference(ownerRef, curr)
	expected.ResourceVersion = curr.ResourceVersion

	if expected.Type == curr.Type &&
		equality.Semantic.DeepDerivative(expected.Annotations, curr.Annotations) &&
		equality.Semantic.DeepDerivative(expected.Labels, curr.Labels) &&
		equality.Semantic.DeepDerivative(expected.OwnerReferences, curr.OwnerReferences) {
		log.V(2).Info("Secret is up-to-date", "namespace", key.Namespace, "name", key.Name)
		return nil
	}

	// Preserve data from the current secret as it's managed by Kubernetes for ServiceAccountToken secrets.
	expected.Data = curr.Data

	log.Info("Updating Secret", "namespace", key.Namespace, "name", key.Name)
	if err := k.client.Update(ctx, expected); err != nil {
		return fmt.Errorf("failed to update Secret %s: %w", key.String(), err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) createSecret(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference) error {
	log.Info("Creating Secret", "name", KEDAPrometheusAuthTriggerSecretName)
	secret := k.expectedSecret(namespace, ownerRef)
	if err := k.client.Create(ctx, secret); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create Secret %s/%s: %w", secret.Namespace, secret.Name, err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) expectedSecret(namespace string, ownerRef metav1.OwnerReference) *corev1.Secret {

	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:            KEDAPrometheusAuthTriggerSecretName,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels:          getKedaLabels(),
			Annotations: map[string]string{
				corev1.ServiceAccountNameKey: KEDAPrometheusAuthServiceAccountName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
		// Data field is populated by the Kubernetes controller for service account tokens.
	}
}

// --- Role ---
func (k *KEDAPrometheusAuthReconciler) reconcileRole(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	expected := k.expectedRole(namespace, ownerRef, annotations)
	curr := &rbacv1.Role{}
	key := client.ObjectKey{Namespace: expected.Namespace, Name: expected.Name}

	err := k.client.Get(ctx, key, curr)
	if err != nil {
		if errors.IsNotFound(err) {
			return k.createRole(ctx, log, namespace, ownerRef, annotations)
		}
		return fmt.Errorf("failed to get Role %s: %w", key.String(), err)
	}

	expected.OwnerReferences = upsertOwnerReference(ownerRef, curr)
	expected.ResourceVersion = curr.ResourceVersion

	if equality.Semantic.DeepDerivative(expected.Rules, curr.Rules) &&
		equality.Semantic.DeepDerivative(expected.Labels, curr.Labels) &&
		equality.Semantic.DeepDerivative(expected.Annotations, curr.Annotations) &&
		equality.Semantic.DeepDerivative(expected.OwnerReferences, curr.OwnerReferences) {
		log.V(2).Info("Role is up-to-date", "namespace", key.Namespace, "name", key.Name)
		return nil
	}

	log.Info("Updating Role", "namespace", key.Namespace, "name", key.Name)
	if err := k.client.Update(ctx, expected); err != nil {
		return fmt.Errorf("failed to update Role %s: %w", key.String(), err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) createRole(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	log.Info("Creating Role", "name", KEDAPrometheusAuthMetricsReaderRoleName)
	role := k.expectedRole(namespace, ownerRef, annotations)
	if err := k.client.Create(ctx, role); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create Role %s/%s: %w", role.Namespace, role.Name, err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) expectedRole(namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:            KEDAPrometheusAuthMetricsReaderRoleName,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels:          getKedaLabels(),
			Annotations:     annotations,
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get"},
			},
			{
				APIGroups: []string{"metrics.k8s.io"},
				Resources: []string{"pods", "nodes"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

// --- RoleBinding ---
func (k *KEDAPrometheusAuthReconciler) reconcileRoleBinding(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	expected := k.expectedRoleBinding(namespace, ownerRef, annotations)
	curr := &rbacv1.RoleBinding{}
	key := client.ObjectKey{Namespace: expected.Namespace, Name: expected.Name}

	err := k.client.Get(ctx, key, curr)
	if err != nil {
		if errors.IsNotFound(err) {
			return k.createRoleBinding(ctx, log, namespace, ownerRef, annotations)
		}
		return fmt.Errorf("failed to get RoleBinding %s: %w", key.String(), err)
	}

	expected.OwnerReferences = upsertOwnerReference(ownerRef, curr)
	expected.ResourceVersion = curr.ResourceVersion

	if equality.Semantic.DeepDerivative(expected.Subjects, curr.Subjects) &&
		equality.Semantic.DeepDerivative(expected.RoleRef, curr.RoleRef) &&
		equality.Semantic.DeepDerivative(expected.Labels, curr.Labels) &&
		equality.Semantic.DeepDerivative(expected.Annotations, curr.Annotations) &&
		equality.Semantic.DeepDerivative(expected.OwnerReferences, curr.OwnerReferences) {
		log.V(2).Info("RoleBinding is up-to-date", "namespace", key.Namespace, "name", key.Name)
		return nil
	}

	log.Info("Updating RoleBinding", "namespace", key.Namespace, "name", key.Name)
	if err := k.client.Update(ctx, expected); err != nil {
		return fmt.Errorf("failed to update RoleBinding %s: %w", key.String(), err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) createRoleBinding(ctx context.Context, log logr.Logger, namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) error {
	log.Info("Creating RoleBinding", "name", KEDAPrometheusAuthMetricsReaderRoleBindingName)
	rb := k.expectedRoleBinding(namespace, ownerRef, annotations)
	if err := k.client.Create(ctx, rb); err != nil && !errors.IsAlreadyExists(err) {
		return fmt.Errorf("failed to create RoleBinding %s/%s: %w", rb.Namespace, rb.Name, err)
	}
	return nil
}

func (k *KEDAPrometheusAuthReconciler) expectedRoleBinding(namespace string, ownerRef metav1.OwnerReference, annotations map[string]string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            KEDAPrometheusAuthMetricsReaderRoleBindingName,
			Namespace:       namespace,
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels:          getKedaLabels(),
			Annotations:     annotations,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      KEDAPrometheusAuthResourceName,
				Namespace: namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     KEDAPrometheusAuthMetricsReaderRoleName,
		},
	}
}

// removeOwnerReferenceFromObject fetches a Kubernetes object and removes a specific owner reference.
// If the object or the owner reference is not found, it's a no-op for that object.
// obj parameter must be a pointer to an empty struct of the target resource kind (e.g., &corev1.ServiceAccount{}).
func (k *KEDAPrometheusAuthReconciler) removeOwnerReferenceFromObject(
	ctx context.Context,
	log logr.Logger,
	obj client.Object,
	namespace string,
	ownerRefToRemove metav1.OwnerReference,
) error {
	key := client.ObjectKey{Namespace: namespace, Name: obj.GetName()}
	err := k.client.Get(ctx, key, obj)
	if err != nil {
		if errors.IsNotFound(err) || meta.IsNoMatchError(err) {
			log.V(1).Info("Resource not found, skipping owner reference removal", "kind", obj.GetObjectKind(), "namespace", key.Namespace, "name", key.Name)
			return nil // No-op: resource doesn't exist.
		}
		return fmt.Errorf("failed to get %s %s for owner reference removal: %w", obj.GetObjectKind(), key.String(), err)
	}

	originalOwnerReferences := obj.GetOwnerReferences()
	if len(originalOwnerReferences) == 0 {
		log.V(1).Info("Resource has no owner references, skipping removal attempt", "kind", obj.GetObjectKind(), "resourceName", obj.GetName(), "namespace", obj.GetNamespace())
		return nil // No-op: resource has no owners.
	}

	newOwnerReferences := make([]metav1.OwnerReference, 0, len(originalOwnerReferences))

	for _, ref := range originalOwnerReferences {
		// Match by UID, as this is the unique identifier for an owner reference instance.
		if ref.UID == ownerRefToRemove.UID {
			log.V(1).Info("Matching owner reference found, will be removed.",
				"ownerRefUID", ref.UID, "resourceKind", obj.GetObjectKind(), "resourceName", obj.GetName())
		} else {
			newOwnerReferences = append(newOwnerReferences, ref)
		}
	}

	if equality.Semantic.DeepEqual(newOwnerReferences, originalOwnerReferences) {
		return nil
	}

	obj.SetOwnerReferences(newOwnerReferences)
	if err := k.client.Update(ctx, obj); err != nil && !errors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		return fmt.Errorf("failed to update %s %s after removing owner reference: %w", obj.GetObjectKind(), key.String(), err)
	}
	log.Info("Successfully removed owner reference and updated resource",
		"resourceKind", obj.GetObjectKind(), "resourceName", obj.GetName(), "namespace", obj.GetNamespace())

	return nil
}

func upsertOwnerReference(expected metav1.OwnerReference, obj client.Object) []metav1.OwnerReference {
	references := obj.GetOwnerReferences()
	newReferences := make([]metav1.OwnerReference, 0, len(references)+1)
	found := false
	for _, ref := range references {
		if ref.APIVersion == expected.APIVersion && ref.Kind == expected.Kind && ref.Name == expected.Name {
			newReferences = append(newReferences, expected) // Replace with the new reference to update UID etc.
			found = true
		} else {
			newReferences = append(newReferences, ref)
		}
	}

	if !found {
		newReferences = append(newReferences, expected)
	}
	return newReferences
}

func getKedaLabels() map[string]string {
	return map[string]string{
		KEDAResourcesLabelKey: KEDAResourcesLabelValue,
	}
}

func retryOnConflicts(f func() error) error {
	return retry.RetryOnConflict(retry.DefaultRetry, f)
}

func (k *KEDAPrometheusAuthReconciler) resourcesToCleanup() []client.Object {
	return []client.Object{
		&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: KEDAPrometheusAuthServiceAccountName}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: KEDAPrometheusAuthTriggerSecretName}},
		&rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: KEDAPrometheusAuthMetricsReaderRoleName}},
		&rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: KEDAPrometheusAuthMetricsReaderRoleBindingName}},
		&kedaapi.TriggerAuthentication{ObjectMeta: metav1.ObjectMeta{Name: KEDAPrometheusAuthTriggerAuthName}},
	}
}
