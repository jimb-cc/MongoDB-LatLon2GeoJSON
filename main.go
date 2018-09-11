package main

// resources:
// - https://www.compose.com/articles/mongodb-and-go-moving-on-from-mgo/
// - https://gitlab.com/wemgl/todocli/blob/master/main.go
// - https://godoc.org/github.com/mongodb/mongo-go-driver/bson

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mongodb/mongo-go-driver/bson"
	"github.com/mongodb/mongo-go-driver/mongo"
)

// define connection URI
const uri = "mongodb://localhost"

// define database
const database = "AIS"

// define source and destination collections
const source = "ais_1m"
const dest = "ais_1m_fix"

// How many docs to read and write at once as part of a bulk insert
const batchSize = 1000

// how many docs have we got left to process
var docsLeft int

func main() {
	fmt.Println("\n----------------------------------\n-- Lat/Lon conversion to GeoJSON |\n----------------------------------")

	// create a context. I have no idea what this means. at all
	ctx := context.Background()

	// create a client for the DB
	client, err := mongo.NewClient(uri)
	if err != nil {
		log.Fatal(err)
	}

	// Connect the client to the DB
	err = client.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// select the database to use
	db := client.Database(database)

	// drop the destination collection (naughty! must deal with errors!)
	_ = db.Collection(dest).Drop(ctx, nil)
	_ = db.Collection("error").Drop(ctx, nil)

	// note the start time
	start := time.Now()
	fmt.Printf("\n--Started at: %v\n", start)

	// find out how many docs we have to process
	countDocs(ctx, db)

	// start doing work
	processDocs(ctx, db, batchSize)

	// note the end time
	finished := time.Now()
	fmt.Printf("\n--Finished at: %v\n", finished)
	fmt.Printf("--took: %v\n", (finished.Sub(start)))

}

func countDocs(ctx context.Context, db *mongo.Database) error {
	// count the number of documents in the source collection
	count, err := db.Collection(source).Count(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	fmt.Printf("--there are %v docs in the collection\n\n", count)

	docsLeft = int(count)

	return nil
}

func processDocs(ctx context.Context, db *mongo.Database, batchSize int) error {

	fmt.Printf("--starting to process documents in batches of %v\n", batchSize)

	// find all the docs in the source collection
	c, err := db.Collection(source).Find(ctx, nil)
	if err != nil {
		return fmt.Errorf("Couldn't find any data: %v", err)
	}
	defer c.Close(ctx)

	//create a slice to hold the batch of documents in.

	for docsLeft > 0 {

		var docs []interface{}
		for i := 0; i < batchSize; i++ {
			c.Next(ctx)
			// load that document into a bson.NewDocument object
			doc := bson.NewDocument()

			if err = c.Decode(doc); err != nil {
				//return fmt.Errorf("can't decode a doc: %v", err)
			}

			// test to see if this document actually has a longitude value, some don't
			if doc.Lookup("Longitude") != nil {
				// and if does append a sub document to it made up from the values (VC - Value constructor) for Long and Lat.  Be careful to watch the order of Lon/Lat
				doc.Append(
					bson.EC.SubDocumentFromElements("coordinates",
						bson.EC.String("type", "Point"),
						bson.EC.ArrayFromElements("coordinates", bson.VC.Decimal128(doc.Lookup("Longitude").Decimal128()), bson.VC.Decimal128(doc.Lookup("Latitude").Decimal128()))))

				// now, delete the two fields that are no longer needed.
				doc.Delete("Longitude")
				doc.Delete("Latitude")

			} else {
				// if you want, uncomment the below to see which records have no lon/lat
				// badDataLog(ctx, db, doc)
				// fmt.Printf("--FAIL-FAIL-FAIL - %v has no lat/lon!\n", doc.Lookup("MMSI").Int32())
			}

			docs = append(docs, doc)
		}

		docsLeft = docsLeft - batchSize
		fmt.Printf("%v Docs left to process\n", docsLeft)

		_, err = db.Collection(dest).InsertMany(ctx, docs)

	}

	if err = c.Err(); err != nil {
		return fmt.Errorf("all data couldn't be listed: %v", err)
	}

	return nil
}

func badDataLog(ctx context.Context, db *mongo.Database, doc *bson.Document) {

	badDoc := bson.NewDocument()

	if doc.Lookup("ShipName") != nil {
		badDoc.Append(bson.EC.String("ShipName", doc.Lookup("ShipName").StringValue()), bson.EC.Int32("MMSI", doc.Lookup("MMSI").Int32()))
	} else {
		badDoc.Append(bson.EC.Int32("MMSI", doc.Lookup("MMSI").Int32()))
	}

	_, _ = db.Collection("error").InsertOne(ctx, badDoc)

}
