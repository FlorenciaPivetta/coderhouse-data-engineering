
output "stream_name" {
  description = "Name of the Kinesis input data stream"
  value       = aws_kinesis_stream.input.name
}

output "stream_arn" {
  description = "ARN of the Kinesis input data stream"
  value       = aws_kinesis_stream.input.arn
}

output "input_stream_arn" {
  description = "ARN of the Kinesis input stream"
  value       = aws_kinesis_stream.input.arn
}

output "input_stream_name" {
  description = "Name of the Kinesis input stream"
  value       = aws_kinesis_stream.input.name
}

output "flink_stateful_output_v2_stream_arn" {
  description = "ARN of the Flink stateful output stream"
  value       = aws_kinesis_stream.flink_stateful_output_v2.arn
}

output "flink_stateful_output_v2_stream_name" {
  description = "Name of the Flink stateful output stream"
  value       = aws_kinesis_stream.flink_stateful_output_v2.name
}
