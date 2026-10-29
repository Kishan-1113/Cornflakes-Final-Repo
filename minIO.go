package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func getMinIOConfig() (endpoint, accessKey, secretKey, bucket string, useSSL bool) {
	endpoint = envOrDefault("MINIO_ENDPOINT", "minio:9000")
	accessKey = envOrDefault("MINIO_ROOT_USER", "minioadmin")
	secretKey = envOrDefault("MINIO_ROOT_PASSWORD", "minioadmin123")
	bucket = envOrDefault("MINIO_BACKUP_BUCKET", "cornflakes-backups")
	useSSL = os.Getenv("MINIO_USE_SSL") == "true"
	return
}

func NewMinIOClient() (*minio.Client, error) {
	endpoint, accessKey, secretKey, _, useSSL := getMinIOConfig()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("init minio client: %w", err)
	}
	return client, nil
}

func EnsureMinIOBucket(ctx context.Context) error {
	client, err := NewMinIOClient()
	if err != nil {
		return err
	}
	_, _, _, bucket, _ := getMinIOConfig()

	exists, err := client.BucketExists(ctx, bucket)
	if err != nil {
		return fmt.Errorf("check bucket %s: %w", bucket, err)
	}
	if !exists {
		err = client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{})
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		log.Printf("Successfully created MinIO bucket: %s", bucket)
	} else {
		log.Printf("MinIO bucket %s already exists", bucket)
	}
	return nil
}
