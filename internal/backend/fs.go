// Copyright 2018 Francisco Souza. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package backend

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// StorageFS is an implementation of the backend storage that stores data on disk
type StorageFS struct {
	rootDir string
	mtx     sync.RWMutex
}

const (
	dirMode  = 0700
	fileMode = 0664
)

// NewStorageFS creates an instance of StorageMemory
func NewStorageFS(objects []Object, rootDir string) (Storage, error) {
	if !strings.HasSuffix(rootDir, "/") {
		rootDir += "/"
	}
	s := &StorageFS{
		rootDir: rootDir,
	}
	for _, o := range objects {
		err := s.CreateObject(o)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// CreateBucket creates a bucket
func (s *StorageFS) CreateBucket(name string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	return s.createBucket(name)
}

func (s *StorageFS) createBucket(name string) error {
	return os.MkdirAll(s.buildBucketPath(name), dirMode)
}

// ListBuckets lists buckets
func (s *StorageFS) ListBuckets() ([]string, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	infos, err := ioutil.ReadDir(s.rootDir)
	if err != nil {
		return nil, err
	}

	buckets := []string{}
	for _, info := range infos {
		if info.IsDir() {
			unescaped, err := url.PathUnescape(info.Name())
			if err != nil {
				return nil, fmt.Errorf("failed to unescape object name %s: %s", info.Name(), err)
			}
			buckets = append(buckets, unescaped)
		}
	}
	return buckets, nil
}

// GetBucket checks if a bucket exists
func (s *StorageFS) GetBucket(name string) error {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	_, err := os.Stat(s.buildBucketPath(name))
	return err
}

// CreateObject stores an object
func (s *StorageFS) CreateObject(obj Object) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	err := s.createBucket(obj.BucketName)
	if err != nil {
		return err
	}

	encoded, err := json.Marshal(obj)
	if err != nil {
		return err
	}

	dirPath, objectPath := s.buildObjectPath(obj.BucketName, obj.Name)

	_, err = os.Stat(dirPath)
	if os.IsNotExist(err) {
		os.MkdirAll(dirPath, dirMode)
	}

	return ioutil.WriteFile(objectPath, encoded, fileMode)
}

// ListObjects lists the objects in a given bucket with a given prefix and delimeter
func (s *StorageFS) ListObjects(bucketName string) ([]Object, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()

	bucketPath := s.buildBucketPath(bucketName)

	objects, err := s.getObjectsFromDir(bucketName, bucketPath)
	if err != nil {
		return nil, err
	}

	return objects, nil
}

// GetObject get an object by bucket and name
func (s *StorageFS) GetObject(bucketName, objectName string) (Object, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.getObject(bucketName, objectName)
}

func (s *StorageFS) getObject(bucketName, objectName string) (Object, error) {
	_, objectPath := s.buildObjectPath(bucketName, objectName)

	encoded, err := ioutil.ReadFile(objectPath)
	if err != nil {
		return Object{}, err
	}
	var obj Object
	err = json.Unmarshal(encoded, &obj)
	if err != nil {
		return Object{}, err
	}
	obj.Name = objectName
	obj.BucketName = bucketName
	return obj, nil
}

// DeleteObject deletes an object by bucket and name
func (s *StorageFS) DeleteObject(bucketName, objectName string) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if objectName == "" {
		return fmt.Errorf("can't delete object with empty name")
	}

	_, objectPath := s.buildObjectPath(bucketName, objectName)
	return os.Remove(objectPath)
}

func (s *StorageFS) getObjectsFromDir(
	bucketName string,
	path string,
) ([]Object, error) {
	fileInfos, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}

	objects := []Object{}
	for _, fileInfo := range fileInfos {
		unescapedPath, err := url.PathUnescape(fileInfo.Name())
		if err != nil {
			return nil, fmt.Errorf(
				"failed to unescape object name %s: %s", fileInfo.Name(), err,
			)
		}

		path := filepath.Join(path, unescapedPath)

		if fileInfo.IsDir() {
			deepObjects, err := s.getObjectsFromDir(bucketName, path)
			if err != nil {
				return nil, err
			}

			objects = append(objects, deepObjects...)
			continue
		}

		bucketPath := s.buildBucketPath(bucketName)
		objectName := strings.ReplaceAll(path, bucketPath+"/", "")

		object, err := s.getObject(bucketName, objectName)
		if err != nil {
			return nil, err
		}

		objects = append(objects, object)
	}

	return objects, nil
}

func (s *StorageFS) buildObjectPath(
	bucketName string,
	objectName string,
) (string, string) {
	cleanName := filepath.Clean(objectName)
	dirName, fileName := filepath.Split(cleanName)

	dirs := strings.Split(dirName, "/")

	dirPath := s.buildBucketPath(bucketName)
	for _, dir := range dirs {
		if dir != "" {
			dirPath = filepath.Join(dirPath, url.PathEscape(dir))
		}
	}

	objectPath := filepath.Join(dirPath, url.PathEscape(fileName))

	return dirPath, objectPath
}

func (s *StorageFS) buildBucketPath(bucketName string) string {
	return filepath.Join(s.rootDir, url.PathEscape(bucketName))
}
