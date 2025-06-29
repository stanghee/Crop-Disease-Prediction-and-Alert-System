import os
import base64
import boto3
import logging
from botocore.exceptions import ClientError
from io import BytesIO

logger = logging.getLogger(__name__)

def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT')}",
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    )

def ensure_bucket_exists(bucket_name):
    client = get_minio_client()
    try:
        client.head_bucket(Bucket=bucket_name)
    except ClientError:
        client.create_bucket(Bucket=bucket_name)

def upload_image(bucket_name, key, image_bytes):
    client = get_minio_client()
    client.put_object(Bucket=bucket_name, Key=key, Body=image_bytes)


class MinIOManager:
    """Enhanced MinIO manager for satellite image storage with metadata support"""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str):
        self.endpoint = endpoint
        self.access_key = access_key
        self.secret_key = secret_key
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        self.client = boto3.client(
            "s3",
            endpoint_url=f"http://{endpoint}",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        
        # Ensure bucket exists
        self._ensure_bucket_exists()
        logger.info(f"✅ MinIOManager initialized for bucket: {bucket_name}")
    
    def _ensure_bucket_exists(self):
        """Ensure the bucket exists, create if not"""
        try:
            self.client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"✅ Bucket {self.bucket_name} exists")
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    self.client.create_bucket(Bucket=self.bucket_name)
                    logger.info(f"✅ Bucket {self.bucket_name} created")
                except ClientError as create_error:
                    logger.error(f"❌ Failed to create bucket {self.bucket_name}: {create_error}")
            else:
                logger.error(f"❌ Error checking bucket {self.bucket_name}: {e}")
    
    def store_image_from_base64(self, image_base64: str, object_name: str, metadata: dict = None) -> bool:
        """
        Store a base64 encoded image in MinIO with optional metadata
        
        Args:
            image_base64: Base64 encoded image string
            object_name: Name for the stored object
            metadata: Optional metadata dictionary
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Decode base64 image
            image_bytes = base64.b64decode(image_base64)
            
            # Prepare upload parameters
            upload_params = {
                'Bucket': self.bucket_name,
                'Key': object_name,
                'Body': BytesIO(image_bytes),
                'ContentType': 'image/png'
            }
            
            # Add metadata if provided
            if metadata:
                # Convert all metadata values to strings and add x-amz-meta- prefix
                meta_formatted = {f"x-amz-meta-{k}": str(v) for k, v in metadata.items()}
                upload_params['Metadata'] = {k.replace('x-amz-meta-', ''): v for k, v in meta_formatted.items()}
            
            # Upload to MinIO
            self.client.put_object(**upload_params)
            
            logger.info(f"✅ Image stored successfully: {object_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to store image {object_name}: {e}")
            return False
    
    def get_image_info(self, object_name: str) -> dict:
        """Get information about a stored image"""
        try:
            response = self.client.head_object(Bucket=self.bucket_name, Key=object_name)
            return {
                'size': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'metadata': response.get('Metadata', {})
            }
        except Exception as e:
            logger.error(f"❌ Failed to get image info for {object_name}: {e}")
            return {}
    
    def list_images(self, prefix: str = "") -> list:
        """List all images in the bucket with optional prefix filter"""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            objects = response.get('Contents', [])
            return [obj['Key'] for obj in objects]
            
        except Exception as e:
            logger.error(f"❌ Failed to list images: {e}")
            return []