import boto3
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KinesisAutoscaler:
    def __init__(self, s3_bucket: str, s3_key: str, threshold: float = 0.1, 
                 sender_email: str = None, recipient_emails: List[str] = None):
        """
        Initialize Kinesis Autoscaler
        
        Args:
            s3_bucket: S3 bucket containing config file
            s3_key: S3 key path to config file
            threshold: Traffic increase threshold (default 0.1 for 10%)
            sender_email: SES verified sender email
            recipient_emails: List of recipient emails for alerts
        """
        self.kinesis_client = boto3.client('kinesis')
        self.cloudwatch_client = boto3.client('cloudwatch')
        self.s3_client = boto3.client('s3')
        self.ses_client = boto3.client('ses')
        self.dynamodb_client = boto3.client('dynamodb')
        
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.threshold = threshold
        self.sender_email = sender_email
        self.recipient_emails = recipient_emails or []
        
        self.stream_configs = self.load_config_from_s3()
        self.scaling_limit_table = 'kinesis-scaling-tracker'
        
        self._ensure_dynamodb_table()
        
    def _ensure_dynamodb_table(self):
        """Create DynamoDB table if it doesn't exist"""
        try:
            self.dynamodb_client.describe_table(TableName=self.scaling_limit_table)
            logger.info(f"DynamoDB table '{self.scaling_limit_table}' exists")
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                logger.info(f"Creating DynamoDB table '{self.scaling_limit_table}'")
                self.dynamodb_client.create_table(
                    TableName=self.scaling_limit_table,
                    KeySchema=[
                        {'AttributeName': 'stream_name', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    AttributeDefinitions=[
                        {'AttributeName': 'stream_name', 'AttributeType': 'S'},
                        {'AttributeName': 'timestamp', 'AttributeType': 'N'}
                    ],
                    BillingMode='PAY_PER_REQUEST'
                )
                waiter = self.dynamodb_client.get_waiter('table_exists')
                waiter.wait(TableName=self.scaling_limit_table)
                logger.info("DynamoDB table created successfully")
        
    def load_config_from_s3(self) -> Dict:
        """Load stream configurations from S3"""
        try:
            logger.info(f"Loading config from s3://{self.s3_bucket}/{self.s3_key}")
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket,
                Key=self.s3_key
            )
            config = json.loads(response['Body'].read().decode('utf-8'))
            logger.info(f"Loaded configuration for {len(config['streams'])} streams")
            logger.info(f"Using traffic threshold: {self.threshold * 100}%")
            return config['streams']
        except Exception as e:
            logger.error(f"Error loading config from S3: {e}")
            raise
    
    def get_scaling_count_24h(self, stream_name: str) -> int:
        """
        Get number of scaling operations in the last 24 hours
        
        Args:
            stream_name: Name of the Kinesis stream
            
        Returns:
            Count of scaling operations in last 24 hours
        """
        try:
            current_time = int(time.time())
            time_24h_ago = current_time - (24 * 60 * 60)
            
            response = self.dynamodb_client.query(
                TableName=self.scaling_limit_table,
                KeyConditionExpression='stream_name = :stream AND #ts > :time_threshold',
                ExpressionAttributeNames={
                    '#ts': 'timestamp'
                },
                ExpressionAttributeValues={
                    ':stream': {'S': stream_name},
                    ':time_threshold': {'N': str(time_24h_ago)}
                }
            )
            
            count = len(response.get('Items', []))
            logger.info(f"Stream '{stream_name}' has {count} scaling operations in last 24h")
            return count
        except Exception as e:
            logger.error(f"Error querying scaling count: {e}")
            return 0
    
    def record_scaling_operation(self, stream_name: str):
        """Record a scaling operation in DynamoDB"""
        try:
            current_time = int(time.time())
            
            self.dynamodb_client.put_item(
                TableName=self.scaling_limit_table,
                Item={
                    'stream_name': {'S': stream_name},
                    'timestamp': {'N': str(current_time)},
                    'datetime': {'S': datetime.fromtimestamp(current_time).isoformat()}
                }
            )
            logger.info(f"Recorded scaling operation for '{stream_name}'")
        except Exception as e:
            logger.error(f"Error recording scaling operation: {e}")
    
    def send_limit_warning_email(self, stream_name: str, current_count: int):
        """
        Send email alert when approaching scaling limit
        
        Args:
            stream_name: Name of the stream
            current_count: Current scaling count in 24h
        """
        if not self.sender_email or not self.recipient_emails:
            logger.warning("SES email not configured - skipping email alert")
            return
        
        try:
            subject = f" Kinesis Scaling Limit Warning - {stream_name}"
            
            body_text = f"""
Kinesis Autoscaling Limit Warning

Stream: {stream_name}
Current Scaling Count (24h): {current_count}/10
Warning Level: APPROACHING LIMIT

The stream '{stream_name}' has performed {current_count} scaling operations in the last 24 hours.
AWS Kinesis has a limit of 10 scaling operations per stream per rolling 24-hour period.

Remaining scaling operations: {10 - current_count}

Please monitor this stream closely. If the limit is reached, no further scaling will be possible until some operations fall outside the 24-hour window.

Timestamp: {datetime.utcnow().isoformat()}

This is an automated alert from Kinesis Autoscaler.
"""
            
            body_html = f"""
<html>
<head></head>
<body>
    <h2 style="color: #ff9800;"> Kinesis Autoscaling Limit Warning</h2>
    
    <table style="border-collapse: collapse; margin: 20px 0;">
        <tr>
            <td style="padding: 8px; font-weight: bold;">Stream:</td>
            <td style="padding: 8px;">{stream_name}</td>
        </tr>
        <tr>
            <td style="padding: 8px; font-weight: bold;">Current Count (24h):</td>
            <td style="padding: 8px; color: #ff9800; font-weight: bold;">{current_count}/10</td>
        </tr>
        <tr>
            <td style="padding: 8px; font-weight: bold;">Warning Level:</td>
            <td style="padding: 8px; color: #ff5722;">APPROACHING LIMIT</td>
        </tr>
        <tr>
            <td style="padding: 8px; font-weight: bold;">Remaining Operations:</td>
            <td style="padding: 8px;">{10 - current_count}</td>
        </tr>
    </table>
    
    <p>The stream <strong>{stream_name}</strong> has performed <strong>{current_count}</strong> scaling operations in the last 24 hours.</p>
    
    <p>AWS Kinesis has a limit of <strong>10 scaling operations</strong> per stream per rolling 24-hour period.</p>
    
    <p style="background-color: #fff3cd; padding: 15px; border-left: 4px solid #ff9800;">
        <strong>Action Required:</strong><br>
        Please monitor this stream closely. If the limit is reached, no further scaling will be possible until some operations fall outside the 24-hour window.
    </p>
    
    <hr style="margin: 20px 0;">
    <p style="color: #666; font-size: 12px;">
        Timestamp: {datetime.utcnow().isoformat()}<br>
        This is an automated alert from Kinesis Autoscaler.
    </p>
</body>
</html>
"""
            
            response = self.ses_client.send_email(
                Source=self.sender_email,
                Destination={
                    'ToAddresses': self.recipient_emails
                },
                Message={
                    'Subject': {
                        'Data': subject,
                        'Charset': 'UTF-8'
                    },
                    'Body': {
                        'Text': {
                            'Data': body_text,
                            'Charset': 'UTF-8'
                        },
                        'Html': {
                            'Data': body_html,
                            'Charset': 'UTF-8'
                        }
                    }
                }
            )
            
            logger.info(f"Warning email sent successfully for '{stream_name}' - MessageId: {response['MessageId']}")
        except Exception as e:
            logger.error(f"Error sending email alert: {e}")
    
    def check_scaling_limit(self, stream_name: str) -> bool:
        """
        Check if stream can be scaled based on 24h limit
        
        Args:
            stream_name: Name of the stream
            
        Returns:
            True if scaling is allowed, False if limit reached
        """
        scaling_count = self.get_scaling_count_24h(stream_name)
        
        if scaling_count >= 10:
            logger.error(f" Stream '{stream_name}' has reached scaling limit (10/10 in 24h)")
            return False
        
        if scaling_count >= 7:
            logger.warning(f"⚠️  Stream '{stream_name}' approaching limit ({scaling_count}/10 in 24h)")
            self.send_limit_warning_email(stream_name, scaling_count)
        
        return True
    
    def get_current_shard_count(self, stream_name: str) -> int:
        """Get current active shard count for a stream"""
        try:
            response = self.kinesis_client.describe_stream_summary(
                StreamName=stream_name
            )
            shard_count = response['StreamDescriptionSummary']['OpenShardCount']
            logger.info(f"Stream '{stream_name}' current shard count: {shard_count}")
            return shard_count
        except Exception as e:
            logger.error(f"Error getting shard count for {stream_name}: {e}")
            raise
    
    def get_cloudwatch_metrics(self, stream_name: str, metric_name: str, 
                               period: int = 60) -> float:
        """
        Get CloudWatch metrics for Kinesis stream
        
        Args:
            stream_name: Name of the Kinesis stream
            metric_name: Metric name (IncomingBytes or GetRecords.Records)
            period: Period in seconds (default 60)
            
        Returns:
            Sum of the metric value
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=1)
            
            response = self.cloudwatch_client.get_metric_statistics(
                Namespace='AWS/Kinesis',
                MetricName=metric_name,
                Dimensions=[
                    {
                        'Name': 'StreamName',
                        'Value': stream_name
                    }
                ],
                StartTime=start_time,
                EndTime=end_time,
                Period=period,
                Statistics=['Sum']
            )
            
            if response['Datapoints']:
                value = response['Datapoints'][0]['Sum']
                logger.info(f"Stream '{stream_name}' - {metric_name}: {value}")
                return value
            else:
                logger.warning(f"No datapoints for {stream_name} - {metric_name}")
                return 0.0
        except Exception as e:
            logger.error(f"Error getting metrics for {stream_name}: {e}")
            return 0.0
    
    def calculate_new_shard_count(self, current_shards: int) -> int:
        """Calculate 25% increase in shard count"""
        increase = max(1, int(current_shards * 0.25))
        new_count = current_shards + increase
        logger.info(f"Calculated new shard count: {current_shards} + {increase} = {new_count}")
        return new_count
    
    def update_shard_count(self, stream_name: str, target_shard_count: int) -> bool:
        """
        Update Kinesis stream shard count
        
        Args:
            stream_name: Name of the stream
            target_shard_count: Desired shard count
            
        Returns:
            True if successful, False otherwise
        """
        try:
            current_shards = self.get_current_shard_count(stream_name)
            
            if current_shards == target_shard_count:
                logger.info(f"Stream '{stream_name}' already at target: {target_shard_count}")
                return True
            
            if not self.check_scaling_limit(stream_name):
                logger.error(f"Cannot scale '{stream_name}' - 24h limit reached")
                return False
            
            logger.info(f"Updating '{stream_name}' from {current_shards} to {target_shard_count} shards")
            
            self.kinesis_client.update_shard_count(
                StreamName=stream_name,
                TargetShardCount=target_shard_count,
                ScalingType='UNIFORM_SCALING'
            )
            
            self.record_scaling_operation(stream_name)
            
            logger.info(f" Successfully initiated shard count update for '{stream_name}'")
            return True
        except Exception as e:
            logger.error(f"Error updating shard count for {stream_name}: {e}")
            return False
    
    def is_traffic_high(self, stream_name: str, baseline_incoming: float, 
                       baseline_get_records: float) -> bool:
        """
        Check if traffic is still increasing
        
        Args:
            stream_name: Stream name
            baseline_incoming: Baseline IncomingBytes value
            baseline_get_records: Baseline GetRecords value
            
        Returns:
            True if traffic is high, False otherwise
        """
        current_incoming = self.get_cloudwatch_metrics(stream_name, 'IncomingBytes')
        current_get_records = self.get_cloudwatch_metrics(stream_name, 'GetRecords.Records')
        
        incoming_increase = (current_incoming - baseline_incoming) / max(baseline_incoming, 1)
        get_records_increase = (current_get_records - baseline_get_records) / max(baseline_get_records, 1)
        
        logger.info(f"Traffic analysis - Incoming: {incoming_increase:.2%}, GetRecords: {get_records_increase:.2%}, Threshold: {self.threshold:.2%}")
        
        is_high = incoming_increase > self.threshold or get_records_increase > self.threshold
        
        if is_high:
            logger.warning(f"🔥 Traffic is HIGH for '{stream_name}'")
        else:
            logger.info(f" Traffic is NORMAL for '{stream_name}'")
        
        return is_high
    
    def monitor_and_scale(self, stream_name: str, baseline_shards: int):
        """
        Monitor metrics and scale stream based on traffic
        
        Args:
            stream_name: Name of the Kinesis stream
            baseline_shards: Original shard count from config file
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Starting autoscaling for stream: {stream_name}")
        logger.info(f"Baseline shard count: {baseline_shards}")
        logger.info(f"Traffic threshold: {self.threshold * 100}%")
        logger.info(f"{'='*60}\n")
        
        scaling_count = self.get_scaling_count_24h(stream_name)
        logger.info(f"Current scaling operations (24h): {scaling_count}/10")
        
        current_shards = self.get_current_shard_count(stream_name)
        

        baseline_incoming = self.get_cloudwatch_metrics(stream_name, 'IncomingBytes')
        baseline_get_records = self.get_cloudwatch_metrics(stream_name, 'GetRecords.Records')
        
        scaling_iterations = 0
        max_iterations = 10 
        
        while scaling_iterations < max_iterations:
            scaling_iterations += 1
            logger.info(f"\n--- Scaling Iteration {scaling_iterations} for '{stream_name}' ---")
            
            logger.info("Waiting 60 seconds to monitor metrics...")
            time.sleep(60)
            
            if self.is_traffic_high(stream_name, baseline_incoming, baseline_get_records):
                logger.info("Traffic is still high - scaling up by 25%")
                
                new_shard_count = self.calculate_new_shard_count(current_shards)
                
                if self.update_shard_count(stream_name, new_shard_count):
                    current_shards = new_shard_count
                    baseline_incoming = self.get_cloudwatch_metrics(stream_name, 'IncomingBytes')
                    baseline_get_records = self.get_cloudwatch_metrics(stream_name, 'GetRecords.Records')
                else:
                    logger.error("Failed to update shard count - may have hit scaling limit")
                    break
            else:
                logger.info("Traffic has stabilized - monitoring for 5 minutes before scaling down")
                
                stable = True
                for i in range(5):
                    logger.info(f"Stability check {i+1}/5 for '{stream_name}'...")
                    time.sleep(60)
                    
                    if self.is_traffic_high(stream_name, baseline_incoming, baseline_get_records):
                        logger.info("Traffic increased again during stability period")
                        stable = False
                        break
                
                if stable:
                    logger.info(f"Traffic is stable - scaling back to baseline: {baseline_shards} shards")
                    self.update_shard_count(stream_name, baseline_shards)
                    logger.info(f" Autoscaling completed for '{stream_name}'")
                    break
        
        if scaling_iterations >= max_iterations:
            logger.warning(f"  Reached maximum iterations for '{stream_name}' - stopping autoscaling")
    
    def process_stream(self, stream_config: Dict) -> Tuple[str, bool, str]:
        """
        Process a single stream (for threading)
        
        Args:
            stream_config: Stream configuration dict
            
        Returns:
            Tuple of (stream_name, success, message)
        """
        stream_name = stream_config['stream_name']
        baseline_shards = stream_config['shard_count']
        
        try:
            self.monitor_and_scale(stream_name, baseline_shards)
            return (stream_name, True, "Completed successfully")
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            logger.error(f"Error processing stream '{stream_name}': {e}")
            return (stream_name, False, error_msg)
    
    def run(self, max_workers: int = 5):
        """
        Execute autoscaling for all configured streams using threading
        
        Args:
            max_workers: Maximum number of concurrent threads (default 5)
        """
        logger.info("="*70)
        logger.info("🚀 Starting Kinesis Autoscaler")
        logger.info(f"Processing {len(self.stream_configs)} streams with {max_workers} workers")
        logger.info(f"Traffic threshold: {self.threshold * 100}%")
        logger.info("="*70)
        
        results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stream = {
                executor.submit(self.process_stream, stream_config): stream_config['stream_name']
                for stream_config in self.stream_configs
            }
            
            for future in as_completed(future_to_stream):
                stream_name = future_to_stream[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result[1]:
                        logger.info(f" '{result[0]}': {result[2]}")
                    else:
                        logger.error(f" '{result[0]}': {result[2]}")
                except Exception as e:
                    logger.error(f" Exception for '{stream_name}': {e}")
                    results.append((stream_name, False, str(e)))
        
        logger.info("\n" + "="*70)
        logger.info("📊 AUTOSCALING SUMMARY")
        logger.info("="*70)
        
        success_count = sum(1 for r in results if r[1])
        failure_count = len(results) - success_count
        
        logger.info(f"Total Streams: {len(results)}")
        logger.info(f"Successful: {success_count}")
        logger.info(f"Failed: {failure_count}")
        
        for stream_name, success, message in results:
            status = "" if success else ""
            logger.info(f"{status} {stream_name}: {message}")
        
        logger.info("="*70)
        logger.info(" Autoscaling process completed")
        logger.info("="*70)


def lambda_handler(event, context):
    """
    AWS Lambda handler function
    
    Expected event format:
    {
        "s3_bucket": "my-config-bucket",
        "s3_key": "kinesis_config.json",
        "threshold": 0.1,
        "sender_email": "alerts@example.com",
        "recipient_emails": ["admin@example.com"],
        "max_workers": 5
    }
    """
    try:
        s3_bucket = event.get('s3_bucket') or context.get_environment_variable('S3_CONFIG_BUCKET')
        s3_key = event.get('s3_key') or context.get_environment_variable('S3_CONFIG_KEY', 'kinesis_config.json')
        threshold = float(event.get('threshold', 0.1))
        sender_email = event.get('sender_email') or context.get_environment_variable('SES_SENDER_EMAIL')
        recipient_emails = event.get('recipient_emails') or json.loads(
            context.get_environment_variable('SES_RECIPIENT_EMAILS', '[]')
        )
        max_workers = int(event.get('max_workers', 5))
        
        logger.info(f"Configuration: S3={s3_bucket}/{s3_key}, Threshold={threshold*100}%, Workers={max_workers}")
        
        autoscaler = KinesisAutoscaler(
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            threshold=threshold,
            sender_email=sender_email,
            recipient_emails=recipient_emails
        )
        
        autoscaler.run(max_workers=max_workers)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Autoscaling completed successfully',
                'threshold': threshold,
                'streams_processed': len(autoscaler.stream_configs)
            })
        }
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e)
            })
        }


if __name__ == "__main__":
    import os
    
    s3_bucket = os.getenv('S3_CONFIG_BUCKET', 'my-kinesis-autoscaler-config')
    s3_key = os.getenv('S3_CONFIG_KEY', 'kinesis_config.json')
    threshold = float(os.getenv('TRAFFIC_THRESHOLD', '0.1'))
    sender_email = os.getenv('SES_SENDER_EMAIL')
    recipient_emails = json.loads(os.getenv('SES_RECIPIENT_EMAILS', '[]'))
    
    autoscaler = KinesisAutoscaler(
        s3_bucket=s3_bucket,
        s3_key=s3_key,
        threshold=threshold,
        sender_email=sender_email,
        recipient_emails=recipient_emails
    )
    
    autoscaler.run(max_workers=5)