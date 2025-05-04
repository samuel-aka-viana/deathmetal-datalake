
###############################################################################
# 1) S3 Bucket
###############################################################################

resource "aws_s3_bucket" "csv_bucket" {
  bucket = "csv-batch-bucket"
}

###############################################################################
# 2) IAM Role para o Firehose
###############################################################################

data "aws_iam_policy_document" "firehose_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "firehose_role" {
  name               = "firehose_to_s3_role"
  assume_role_policy = data.aws_iam_policy_document.firehose_assume.json
}

data "aws_iam_policy_document" "firehose_s3_policy" {
  statement {
    effect    = "Allow"
    actions   = ["s3:*", "kinesis:*"]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "firehose_s3" {
  name   = "firehose_s3_policy"
  role   = aws_iam_role.firehose_role.id
  policy = data.aws_iam_policy_document.firehose_s3_policy.json
}

###############################################################################
# 3) Kinesis Data Streams
###############################################################################

resource "aws_kinesis_stream" "albums_stream" {
  name        = "albums-stream"
  shard_count = 1
}

resource "aws_kinesis_stream" "bands_stream" {
  name        = "bands-stream"
  shard_count = 1
}

resource "aws_kinesis_stream" "reviews_stream" {
  name        = "reviews-stream"
  shard_count = 1
}

###############################################################################
# 4) Firehose Delivery Streams → S3
###############################################################################

resource "aws_kinesis_firehose_delivery_stream" "albums_firehose" {
  name        = "albums-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.albums_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.csv_bucket.arn
    prefix              = "landing/albums/"
    buffering_size      = 5
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
    error_output_prefix = "errors/albums/!{firehose:yyyy/MM/dd}/"
  }
}

resource "aws_kinesis_firehose_delivery_stream" "bands_firehose" {
  name        = "bands-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.bands_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.csv_bucket.arn
    prefix              = "landing/bands/"
    buffering_size      = 5
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
    error_output_prefix = "errors/bands/!{firehose:yyyy/MM/dd}/"
  }
}

resource "aws_kinesis_firehose_delivery_stream" "reviews_firehose" {
  name        = "reviews-firehose"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.reviews_stream.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn            = aws_iam_role.firehose_role.arn
    bucket_arn          = aws_s3_bucket.csv_bucket.arn
    prefix              = "landing/reviews/"
    buffering_size      = 5
    buffering_interval  = 60
    compression_format  = "UNCOMPRESSED"
    error_output_prefix = "errors/reviews/!{firehose:yyyy/MM/dd}/"
  }
}

###############################################################################
# 5) Outputs úteis
###############################################################################

output "bucket_name" {
  value = aws_s3_bucket.csv_bucket.bucket
}

output "albums_stream_name" {
  value = aws_kinesis_stream.albums_stream.name
}

output "bands_stream_name" {
  value = aws_kinesis_stream.bands_stream.name
}

output "reviews_stream_name" {
  value = aws_kinesis_stream.reviews_stream.name
}

output "albums_firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.albums_firehose.name
}

output "bands_firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.bands_firehose.name
}

output "reviews_firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.reviews_firehose.name
}
