# A single-node deployment: one instance runs the Docker Compose stack, with S3 in
# place of the local object store. For a monitoring workload this size (a few
# hundred trains, tens of KB/s) a managed Kafka + Flink setup would cost several
# times more than the work needs; docs/cost.md has the comparison.

# ---------- storage ----------
resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  # Raw feed segments are only read for replays and backfills: cheap storage after a month.
  rule {
    id     = "raw-archive"
    status = "Enabled"
    filter {
      prefix = "raw/"
    }
    transition {
      days          = var.raw_archive_glacier_days
      storage_class = "GLACIER_IR"
    }
    expiration {
      days = var.raw_archive_expire_days
    }
  }

  # Old Flink checkpoints are useless once newer ones exist.
  rule {
    id     = "checkpoints"
    status = "Enabled"
    filter {
      prefix = "checkpoints/"
    }
    expiration {
      days = 7
    }
  }

  # Failed multipart uploads otherwise sit there and get billed.
  rule {
    id     = "abort-incomplete-uploads"
    status = "Enabled"
    filter {}
    abort_incomplete_multipart_upload {
      days_after_initiation = 2
    }
  }
}

# ---------- compute ----------
data "aws_ami" "al2023_arm" {
  most_recent = true
  owners      = ["amazon"]
  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-arm64"]
  }
}

data "aws_iam_policy_document" "assume_ec2" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["ec2.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "node" {
  name               = "subwaypulse-node"
  assume_role_policy = data.aws_iam_policy_document.assume_ec2.json
}

data "aws_iam_policy_document" "lake_access" {
  statement {
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
}

resource "aws_iam_role_policy" "lake_access" {
  name   = "lake-access"
  role   = aws_iam_role.node.id
  policy = data.aws_iam_policy_document.lake_access.json
}

# Shell access through SSM Session Manager, so no SSH port is open.
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.node.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "node" {
  name = "subwaypulse-node"
  role = aws_iam_role.node.name
}

resource "aws_security_group" "node" {
  name        = "subwaypulse-node"
  description = "Outbound only; dashboards are reached through an SSM port forward"

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "node" {
  ami                    = data.aws_ami.al2023_arm.id
  instance_type          = var.instance_type
  iam_instance_profile   = aws_iam_instance_profile.node.name
  vpc_security_group_ids = [aws_security_group.node.id]

  root_block_device {
    volume_type = "gp3"
    volume_size = 40
    encrypted   = true
  }

  metadata_options {
    http_tokens = "required"
  }

  user_data = <<-EOT
    #!/bin/bash
    set -e
    dnf install -y docker git
    systemctl enable --now docker
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -fsSL https://github.com/docker/compose/releases/download/v2.32.1/docker-compose-linux-aarch64 \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
  EOT

  tags = {
    Name = "subwaypulse"
  }
}

# ---------- guardrails ----------
resource "aws_budgets_budget" "monthly" {
  name         = "subwaypulse-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "ACTUAL"
    subscriber_email_addresses = [var.alert_email]
  }
}
