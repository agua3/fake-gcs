package main

import (
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path"

	"github.com/agua3/fake-gcs/fakestorage"
)

const (
	defaultStorageRoot string = "storage"
)

const (
	serverHost string = "0.0.0.0"
	serverPort uint16 = 4443
)

var (
	storageRoot = getEnv("STORAGE_ROOT", defaultStorageRoot)
	rootDir     = path.Join("/", storageRoot)
)

func getEnv(env string, defaultValue string) string {
	if value, exists := os.LookupEnv(env); exists {
		return value
	}

	return defaultValue
}

func getObjectsFromPath(
	bucketName string, objectsPath string, name string,
) []fakestorage.Object {
	files, err := ioutil.ReadDir(objectsPath)

	if err != nil {
		panic(err)
	}

	objects := []fakestorage.Object{}
	for _, file := range files {
		unescapedName, _ := url.PathUnescape(file.Name())

		objectName := path.Join(name, unescapedName)
		objectPath := path.Join(objectsPath, unescapedName)

		if file.IsDir() {
			objects = append(
				objects,
				getObjectsFromPath(bucketName, objectPath, objectName)...,
			)
			continue
		}

		fileContent, err := ioutil.ReadFile(objectPath)

		if err != nil {
			panic(err)
		}

		object := fakestorage.Object{
			BucketName: bucketName,
			Name:       objectName,
			Content:    fileContent,
		}

		objects = append(objects, object)
	}

	return objects
}

// The buckets are the first level of folders...
// Then are part of that bucket but needs to be loaded
func generateObjectsFromFiles() []fakestorage.Object {
	objects := []fakestorage.Object{}

	// if a storage volume is mounted in the container
	if _, err := os.Stat(rootDir); !os.IsNotExist(err) {
		buckets, err := ioutil.ReadDir(rootDir)

		if err != nil {
			panic(err)
		}

		for _, bucket := range buckets {
			unescapedBucketName, _ := url.PathUnescape(bucket.Name())
			bucketPath := path.Join(rootDir, unescapedBucketName)

			if bucket.IsDir() {
				objects = append(
					objects, getObjectsFromPath(unescapedBucketName, bucketPath, "")...,
				)

				fmt.Printf("Found bucket `%s`\n", bucket.Name())
				fmt.Printf("Add %d file/s in `%s`\n", len(objects), bucket.Name())
			}
		}
	}

	return objects
}

func main() {
	loadedObjects := generateObjectsFromFiles()

	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{
		InitialObjects: loadedObjects,
		Host:           serverHost,
		Port:           serverPort,
		StorageRoot:    rootDir,
	})

	if err != nil {
		panic(err)
	}

	fmt.Printf("Server started at %s\n", server.URL())

	select {}
}
