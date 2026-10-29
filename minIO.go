package main

import (
	"context"
	"log"
	"os"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	endpoint        = "minio:9000"
	accessKeyID     = "minioadmin"
	secretAccessKey = "minioadmin123"
	useSSL          = false
	bucketName      = "local-backups"
	localFilePath   = "any dummy file path as of now"
	objectName      = "backups/2026-10-29-backup.tar.gz"
)

func bakupToMinIO() {
	ctx := context.Background()

	// 1. Initialize MinIO Client
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln("Failed to initialize MinIO client:", err)
	}
	log.Printf("Successfully connected to MinIO at %s\n", endpoint)

	// 2. Ensure the backup bucket exists
	err = minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		// Check if the bucket already exists
		exists, errBucketExists := minioClient.BucketExists(ctx, bucketName)
		if errBucketExists == nil && exists {
			log.Printf("Bucket '%s' already exists.\n", bucketName)
		} else {
			log.Fatalln("Failed to create bucket:", err)
		}
	} else {
		log.Printf("Successfully created bucket '%s'.\n", bucketName)
	}

	// 3. Open local file to get its stats (size & content type)
	file, err := os.Open(localFilePath)
	if err != nil {
		log.Fatalln("Failed to open local backup file:", err)
	}
	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		log.Fatalln("Failed to read file stats:", err)
	}

	// 4. Upload the backup stream using PutObject
	log.Printf("Uploading %s (%d bytes) to MinIO...", localFilePath, fileStat.Size())

	uploadInfo, err := minioClient.PutObject(ctx, bucketName, objectName, file, fileStat.Size(), minio.PutObjectOptions{
		ContentType: "application/gzip", // Adjust based on file type
	})
	if err != nil {
		log.Fatalln("Failed to upload backup:", err)
	}

	log.Printf("\nSuccessfully backed up file! ETag: %s\nSize: %d bytes\n", uploadInfo.ETag, uploadInfo.Size)
}
