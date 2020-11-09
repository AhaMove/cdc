package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Supplier struct {
	ID     string `json:"_id"`
	Name   string `json:"name"`
	Email  string `json:"email"`
	Status string `json:"status"`
}

func main() {
	ctx, _ := context.WithTimeout(context.Background(), 1000*time.Second)
	clientOptions := options.Client()
	clientOptions.SetMaxPoolSize(20)
	clientOptions.SetMinPoolSize(1)
	client, err := mongo.Connect(ctx, clientOptions.ApplyURI("mongodb+srv://admin:Aha2020@ahamove-sz8j3.mongodb.net/ahamove")) // fill mongo_connection_url
	if err != nil {
		fmt.Printf("Connect to mongodb failed: %v\n", err)
		os.Exit(1)
	}

	defer client.Disconnect(ctx)
	collection := client.Database("ahamove").Collection("supplier")
	status := "ONLINE"
	var wg sync.WaitGroup
	for i := 0; i < 100000; i++ {
		if status == "ONLINE" {
			status = "OFFLINE"
		} else {
			status = "ONLINE"
		}
		wg.Add(1)
		go updateStatusSupplier(ctx, status, collection, &wg)
	}
	wg.Wait()
}

func updateStatusSupplier(ctx context.Context, status string, collection *mongo.Collection, wg *sync.WaitGroup) (err error) {
	defer wg.Done()
	filter := bson.M{"_id": bson.M{"$eq": "84358866124"}} // fill phone_number
	update := bson.M{"$set": bson.M{"status": status}}
	_, err = collection.UpdateOne(ctx, filter, update)
	if err != nil {
		fmt.Printf("update supplier failed: %v\n", err)

	}
	return
}
