resource "aws_s3_bucket" "vitals" {
  bucket = "${lower(var.hospital_name)}-patient-vitals"
  tags   = { Project = "HealthMonitor", Environment = "prod" }
}

resource "aws_s3_bucket_versioning" "vitals" {
  bucket = aws_s3_bucket.vitals.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "vitals" {
  bucket = aws_s3_bucket.vitals.id
  rule {
    id     = "archive-old-vitals"
    status = "Enabled"
    transition {
      days          = 90
      storage_class = "GLACIER"
    }
    expiration { days = 365 }
  }
}

output "s3_bucket_name" { value = aws_s3_bucket.vitals.bucket }
