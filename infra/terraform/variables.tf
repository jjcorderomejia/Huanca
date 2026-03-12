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
