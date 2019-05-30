package main

import (
	"fmt"
	"io/ioutil"
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

// TODO: Needs to be recursive
// The buckets are the first level of folders...
// Then are part of that bucket but needs to be loaded
func generateObjectsFromFiles() []fakestorage.Object {
	objects := []fakestorage.Object{}

	// if a storage volume is mounted in the container
	if _, err := os.Stat(rootDir); !os.IsNotExist(err) {
		// list the content
		files, err := ioutil.ReadDir(rootDir)

		if err != nil {
			panic(err)
		}

		for _, file := range files {
			filePath := path.Join(rootDir, file.Name())
			fileInfo, err := os.Stat(filePath)

			if err != nil {
				panic(err)
			}

			// if it's a directory, it look for the files it's containing
			if fileInfo.IsDir() {
				bucket := file
				bucketPath := filePath

				bucketFiles, err := ioutil.ReadDir(bucketPath)

				if err != nil {
					panic(err)
				}

				fmt.Printf("Found bucket `%s`\n", bucket.Name())

				// for each file create an object
				for _, file := range bucketFiles {
					filePath := path.Join(bucketPath, file.Name())
					fileContent, err := ioutil.ReadFile(filePath)

					if err != nil {
						panic(err)
					}

					fmt.Printf(
						"Creating object `%s` in bucket `%s`\n",
						file.Name(),
						bucket.Name(),
					)

					object := fakestorage.Object{
						BucketName: bucket.Name(),
						Name:       file.Name(),
						Content:    fileContent,
					}

					objects = append(objects, object)
				}
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
