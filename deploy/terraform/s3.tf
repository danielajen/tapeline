# The Iceberg warehouse.

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.lakehouse_bucket_name
  tags   = merge(local.tags, { Name = "${local.name}-lakehouse" })
}

resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Versioning is off, deliberately.
  #
  # Iceberg already provides snapshot isolation and time travel at the table
  # level, which is the property versioning would be bought for. Turning both
  # on means paying to store every version of every Parquet file that Iceberg
  # compaction rewrites — and compaction rewrites a lot. Table history is the
  # right layer for this, not object history.
  versioning_configuration {
    status = "Disabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  # Aborting incomplete multipart uploads is the single highest-value
  # lifecycle rule on any bucket a streaming job writes to. A Flink job that
  # is killed mid-commit leaves parts behind, they are invisible in the
  # console, and they are billed as storage forever.
  rule {
    id     = "abort-incomplete-multipart"
    status = "Enabled"

    filter {}

    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }

  # Age raw data into cheaper storage. Backfills read recent partitions far
  # more often than old ones, and the retrieval characteristics of
  # Intelligent-Tiering suit a workload whose access pattern is genuinely
  # unpredictable.
  rule {
    id     = "tier-cold-data"
    status = "Enabled"

    filter {
      prefix = "warehouse/"
    }

    transition {
      days          = 30
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}
