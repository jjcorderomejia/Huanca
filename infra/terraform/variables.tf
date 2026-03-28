variable "ghcr_token" {
  type      = string
  sensitive = true
}

variable "ghcr_user" {
  type    = string
  default = "jjcorderomejia"
}

variable "bigdata_namespace" {
  type    = string
  default = "bigdata"
}

variable "apps_namespace" {
  type    = string
  default = "apps"
}

# Required when any hcloud_* resource or data source is managed.
# Inject as TF_VAR_hcloud_token — never hardcoded.
variable "hcloud_token" {
  description = "Hetzner Cloud API token."
  type        = string
  sensitive   = true
}
