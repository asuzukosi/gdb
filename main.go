package main

import (
	"encoding/json"
	"fmt"
	"log"
)

type Address struct {
	City    string `json:"city"`
	State   string `json:"state"`
	Country string `json:"country"`
	Pincode string `json:"pincode"`
}

type User struct {
	Name    string  `json:"name"`
	Age     int     `json:"age"`
	Contact string  `json:"contact"`
	Company string  `json:"company"`
	Address Address `json:"address"`
}

func main() {
	var dir string = "./store"

	db, err := CreateGDBDatabase(dir, nil)

	if err != nil {
		log.Fatal(err)
	}

	var employees []User = []User{
		{Name: "Kosi", Age: 21, Contact: "kosi@gmail.com", Company: "inova", Address: Address{City: "Abuja", State: "FCT", Country: "Nigeria", Pincode: "221"}},
		{Name: "david", Age: 22, Contact: "kosi@gmail.com", Company: "inova", Address: Address{City: "Abuja", State: "FCT", Country: "Nigeria", Pincode: "221"}},
		{Name: "simon", Age: 23, Contact: "kosi@gmail.com", Company: "inova", Address: Address{City: "Abuja", State: "FCT", Country: "Nigeria", Pincode: "221"}},
	}

	for _, e := range employees {
		// convert employee into a json string
		db.Write("users", e)
	}

	records, err := db.ReadAll("users")
	if err != nil {
		log.Fatal(err)
	}

	allUsers := []User{}

	for _, record := range records {
		employee := User{}
		b, _ := json.Marshal(record)
		err = json.Unmarshal(b, &employee)
		if err != nil {
			log.Fatal(err)
		}
		allUsers = append(allUsers, employee)
	}
	fmt.Println(allUsers)

	// var id string

	// err = db.Delete("users", id)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// err = db.DeleteAll("users")

	// if err != nil {
	// 	log.Fatal(err)
	// }

	fmt.Println("Welcome to my gdb database")
}
