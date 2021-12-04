/*
This is the GDB database (The go database).

It is the implementation of a mongo db like database that stores its files in a json file
The database stores a new documents by creating a new uuid to represent that document

The databases stores documents in collection whicha are a set of related documents
*/

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"

	"github.com/google/uuid"
	"github.com/jcelliott/lumber"
)

// The current version of gdb
const Version = "1.0.0"

// Create the Logger interface and the database driver
type (
	Logger interface {
		Fatal(string, ...interface{})
		Error(string, ...interface{})
		Warn(string, ...interface{})
		Info(string, ...interface{})
		Debug(string, ...interface{})
		Trace(string, ...interface{})
	}
	Driver struct {
		mutex   sync.Mutex
		mutexes map[string]*sync.Mutex
		dir     string
		log     Logger
	} // Driver is what connects your code and the database
)

// create a struct to store configuration options
type ConfigOptions struct {
	Logger
}

// create a function to creat a new driver
func CreateGDBDatabase(dir string, options *ConfigOptions) (*Driver, error) {
	dir = filepath.Clean(dir)
	opts := ConfigOptions{}

	if options != nil {
		opts = *options
	}

	if opts.Logger == nil {
		opts.Logger = lumber.NewConsoleLogger((lumber.INFO))
	}

	// creat a driver with the logger and the directory
	driver := Driver{
		log:     opts.Logger,
		dir:     dir,
		mutex:   sync.Mutex{},
		mutexes: make(map[string]*sync.Mutex),
	}

	// check if the directory exists
	// if it exists return the driver and no error
	if _, err := os.Stat(dir); err == nil {
		opts.Logger.Debug("using '%s', database already exists", dir)
		return &driver, nil
	} else {
		// if the directory does not exist, create it and return the driver
		opts.Logger.Debug("creating database in '%s'", dir)
		return &driver, os.Mkdir(dir, 0755)
	}
}

func (d *Driver) Write(collection string, v interface{}) error {
	// Check if a valid collection was passed in
	if collection == "" {
		return fmt.Errorf("missing collection - no place to save record")
	}
	// Check if a valid value was passed in
	if v == nil {
		return fmt.Errorf("no data to store in the database")
	}

	// Create a new unique uuid to save the documet
	var id string = uuid.New().String()

	// get or create a mutex for the driver
	mutex := d.getOrCreateMutex(collection)

	// Lock that collection so no other opeations can modify it
	mutex.Lock()

	// set the mutex to unlock when the operation is done
	defer mutex.Unlock()

	// Create the collection directory if not exist
	dir := filepath.Join(d.dir, collection)

	// Create the document within the collection directory
	fnlPath := filepath.Join(dir, id+".json")

	// Create temp file for document path
	//tmpPath := fnlPath + ".tmp"

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// Extract the list of bytes from the value
	b, err := json.Marshal(v)

	if err != nil {
		return err
	}

	// create a map called data
	data := make(map[string]interface{})

	// unmarshal the bytes into the data
	err = json.Unmarshal(b, &data)
	if err != nil {
		return err
	}
	// add a variable '_id' to the data
	data["_id"] = id

	// marshal the data into a list of bytes
	b, err = json.MarshalIndent(data, "", "\t")
	if err != nil {
		return err
	}

	// add an empty lin to the end of the list of bytes
	b = append(b, byte('\n'))

	err = ioutil.WriteFile(fnlPath, b, 0644)
	if err != nil {
		return err
	}
	if err != nil {
		return err
	}
	return nil
}

func (d *Driver) Read(collection string, id string) (map[string]interface{}, error) {
	// Check if collection is not empty

	if collection == "" {
		return nil, errors.New("collection can not be empty")
	}

	// Check if id is not empty
	if id == "" {
		return nil, errors.New("id can not be empty")
	}

	// Get the colleciton directory
	dir := filepath.Join(d.dir, collection, id)

	if _, err := stat(dir); err != nil {
		return nil, err
	}

	// read from the file
	b, err := ioutil.ReadFile(dir + ".json")

	if err != nil {
		return nil, err
	}

	data := make(map[string]interface{})
	err = json.Unmarshal(b, &data)
	if err != nil {
		return nil, err
	}

	return data, nil

}

func (d *Driver) ReadAll(collection string) ([]map[string]interface{}, error) {
	// Check if collection is not empty
	if collection == "" {
		return nil, errors.New("")
	}
	// Get path to the collection directory
	dir := filepath.Join(d.dir, collection)

	// check if the directory exists
	_, err := stat(dir)
	if err != nil {
		return nil, err
	}

	// create a variable to store th list of outputs of the function
	var output []map[string]interface{} = []map[string]interface{}{}

	// open the directory using ioutil.ReadDir
	files, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	// loop through the files and append the files to the output
	for _, file := range files {
		b, err := ioutil.ReadFile(filepath.Join(dir, file.Name()))
		if err != nil {
			return nil, err
		}

		data := make(map[string]interface{})
		// unmarshal the json data and store it in the data variable
		err = json.Unmarshal(b, &data)
		if err != nil {
			return nil, err
		}
		output = append(output, data)
	}
	return output, nil
}

func (d *Driver) Delete(collection string, id string) error {
	// Check if collection is not empty
	if collection == "" {
		return errors.New("collection can not be empty")
	}
	// Check if colleciton already exists
	dir := filepath.Join(d.dir, collection)
	_, err := stat(dir)
	if err != nil {
		return err
	}

	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()

	switch fi, _ := stat(filepath.Join(dir, id)); {
	case fi == nil:
		return errors.New("unable to find document")

	case fi != nil:
		os.RemoveAll(filepath.Join(dir, id+".json"))
	}

	return nil
}

func (d *Driver) DeleteAll(collection string) error {
	// Check if collection is not empty
	if collection == "" {
		return errors.New("collection can not be empty")

	}
	mutex := d.getOrCreateMutex(collection)
	mutex.Lock()
	defer mutex.Unlock()
	// Get directory of collection
	dir := filepath.Join(d.dir, collection)
	_, err := stat(dir)
	if err != nil {
		return errors.New("colleciton not found")
	}

	return os.RemoveAll(dir)

}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	m, ok := d.mutexes[collection]

	if !ok {
		m = &sync.Mutex{}
		d.mutexes[collection] = m
	}
	return m
}

func stat(path string) (fi os.FileInfo, err error) {
	if fi, err = os.Stat(path); os.IsNotExist(err) {
		fi, err = os.Stat(path + ".json")
	}
	return
}
