output "namespaces" {
  value = [
    data.kubernetes_namespace.bigdata.metadata[0].name,
    data.kubernetes_namespace.apps.metadata[0].name,
    data.kubernetes_namespace.argocd.metadata[0].name,
    data.kubernetes_namespace.spark_operator.metadata[0].name,
  ]
  description = "All namespaces — pre-existing, Terraform reads only"
}
