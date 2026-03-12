terraform {
  backend "s3" {
    bucket                      = "tf-state"
    key                         = "fraud-lab/terraform.tfstate"
    region                      = "us-east-1"
    endpoints                   = { s3 = "http://minio.bigdata.svc.cluster.local:9000" }
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_requesting_account_id  = true
    use_path_style              = true
    # Credentials passed via AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env vars
    # Never hardcoded here — injected from minio-secret in K8s Job
  }
}
