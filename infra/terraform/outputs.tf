output "bucket" {
  value = aws_s3_bucket.lake.bucket
}

output "instance_id" {
  description = "Connect with: aws ssm start-session --target <id>"
  value       = aws_instance.node.id
}
