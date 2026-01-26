
variable "project" {
  description = "Project ID"
  default     = "data-zoomcamp-485408"
}

variable "region" {
  description = "Project region"
  default     = "us-central1"
}

variable "zone" {
  description = "Project zone"
  default     = "us-central1-c"
}

variable "location" {
  description = "Project location"
  default     = "US"
}


variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default     = "demo_dataset"
}


variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "data-zoomcamp-485408-demo-bucket"
}
