terraform {
  required_version = ">= 1.5.0"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.20.0"
    }
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = ">= 1.45.0"
    }
  }
}

provider "kubernetes" {}

# ── Hetzner Cloud provider ─────────────────────────────────────────────
# Manages node lifecycle via cloud-init user_data on new Hetzner servers.
# Token injected as TF_VAR_hcloud_token — never hardcoded.
# When re-running the Terraform Job (Phase 0.5), add TF_VAR_hcloud_token
# from the terraform-vars K8s secret.
provider "hcloud" {
  token = var.hcloud_token
}

# ── Namespaces (all pre-existing — Terraform reads only) ─────────────
data "kubernetes_namespace" "bigdata" {
  metadata { name = var.bigdata_namespace }
}

data "kubernetes_namespace" "apps" {
  metadata { name = var.apps_namespace }
}

data "kubernetes_namespace" "argocd" {
  metadata { name = "argocd" }
}

data "kubernetes_namespace" "spark_operator" {
  metadata { name = "spark-operator" }
}

# ── Existing secrets — read only ────────────────────────────────────
data "kubernetes_secret" "ghcr_bigdata" {
  metadata {
    name      = "ghcr-creds"
    namespace = "bigdata"
  }
}

data "kubernetes_secret" "ghcr_apps" {
  metadata {
    name      = "ghcr-creds"
    namespace = "apps"
  }
}

# ── Spark ServiceAccount + RBAC ─────────────────────────────────────
data "kubernetes_service_account" "spark" {
  metadata {
    name      = "spark"
    namespace = "bigdata"
  }
}

resource "kubernetes_cluster_role" "spark_role" {
  metadata { name = "spark-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "services", "configmaps", "secrets",
                  "persistentvolumeclaims"]
    verbs      = ["get", "list", "watch", "create", "delete", "deletecollection", "patch", "update"]
  }
  rule {
    api_groups = ["sparkoperator.k8s.io"]
    resources  = ["sparkapplications", "scheduledsparkapplications"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch", "update"]
  }
}

resource "kubernetes_cluster_role_binding" "spark_binding" {
  metadata { name = "spark-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.spark_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = data.kubernetes_service_account.spark.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

# ── BuildKit ServiceAccount + RBAC ──────────────────────────────────
resource "kubernetes_service_account" "buildkit" {
  metadata {
    name      = "buildkit"
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "buildkit_role" {
  metadata { name = "buildkit-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log"]
    verbs      = ["get", "list", "watch", "create", "delete"]
  }
}

resource "kubernetes_cluster_role_binding" "buildkit_binding" {
  metadata { name = "buildkit-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.buildkit_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.buildkit.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

# ── Airflow ServiceAccount + RBAC ───────────────────────────────────
resource "kubernetes_service_account" "airflow" {
  metadata {
    name      = "airflow"
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}

resource "kubernetes_cluster_role" "airflow_role" {
  metadata { name = "airflow-role" }
  rule {
    api_groups = [""]
    resources  = ["pods", "pods/log", "pods/exec"]
    verbs      = ["get", "list", "watch", "create", "delete", "patch"]
  }
}

resource "kubernetes_cluster_role_binding" "airflow_binding" {
  metadata { name = "airflow-role-binding" }
  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.airflow_role.metadata[0].name
  }
  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.airflow.metadata[0].name
    namespace = data.kubernetes_namespace.bigdata.metadata[0].name
  }
}
