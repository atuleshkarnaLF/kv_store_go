package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

func main() {
	kvs, err := NewKeyValueStore()
	if err != nil {
		fmt.Printf("Failed to create KeyValueStore: %s\n", err)
		os.Exit(1)
	}

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Key-Value Distributed Store using MongoDB")
	fmt.Println("---------------------------------------")

	for {
		fmt.Print("Enter command (PUT/GET/DELETE/REPLICATE/EXIT): ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		switch strings.ToUpper(input) {
		case "PUT":
			fmt.Print("Enter key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			fmt.Print("Enter value: ")
			value, _ := reader.ReadString('\n')
			value = strings.TrimSpace(value)

			err := kvs.Put(key, value)
			if err != nil {
				fmt.Printf("Failed to put key-value pair: %s\n", err)
			} else {
				fmt.Println("Key-value pair added successfully!")
			}

		case "GET":
			fmt.Print("Enter key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)

			value, err := kvs.Get(key)
			if err != nil {
				fmt.Printf("Failed to get value for key: %s\n", err)
			} else {
				fmt.Printf("Value for key '%s': %s\n", key, value)
			}

		case "DELETE":
			fmt.Print("Enter key: ")
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
		
			err := kvs.Delete(key)
			if err != nil {
				fmt.Printf("Failed to delete key-value pair: %s\n", err)
			} else {
				fmt.Println("Key-value pair deleted successfully!")
		}
	
		case "REPLICATE":
			newNode, err := NewKeyValueStore()
			if err != nil {
				fmt.Printf("Failed to create new KeyValueStore: %s\n", err)
				break
			}
		
			err = kvs.Replicate(newNode)
			if err != nil {
				fmt.Printf("Failed to replicate key-value pairs: %s\n", err)
			} else {
				fmt.Println("Key-value pairs replicated successfully!")
			}
	
		case "EXIT":
			fmt.Println("Exiting...")
			os.Exit(0)

		default:
			fmt.Println("Invalid command. Please try again.")
		}

		fmt.Println()
	}
}
