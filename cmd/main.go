package main

import (
	"context"
	"fmt"
	"github.com/alecthomas/kong"
	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"time"
)

const (
	databaseName   = "myNewDB"
	collectionName = "people"
)

type commandContext struct {
	Debug bool `optional:"" default:"false"`

	logger zerolog.Logger
}

type commonFlags struct {
	ConnectionString string `required:"" help:"Full connection string to a DocumentDB. Example: mongodb://<username>:<password>@<host>:27017/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false"`
}

type runProducerCmd struct {
	commonFlags
	Inserts             int   `required:"" help:"Number of records to insert." default:"1"`
	Updates             int   `required:"" help:"Number of updates to perform on each inserted record." default:"10"`
	DelayBetweenInserts int64 `help:"Number of milliseconds to pause before inserting another record" default:"10"`
	DelayBetweenUpdates int64 `help:"Number of milliseconds to pause before performing an update" default:"10"`
}

type runConsumerCmd struct {
	commonFlags
}

type commandLine struct {
	Debug       bool           `help:"Debug mode"`
	RunProducer runProducerCmd `cmd:"" help:"Runs a producer that inserts and updates records in a DocumentDB."`
	RunConsumer runConsumerCmd `cmd:"" help:"Runs a change-stream consumer that output records from a DocumentDB change-stream."`
}

type person struct {
	FirstName string
	LastName  string
	Created   time.Time
	Modified  time.Time
}

func main() {
	cli := &commandLine{}
	kongContext := kong.Parse(cli)
	err := kongContext.Run(&commandContext{
		Debug:  cli.Debug,
		logger: getLogger(cli.Debug),
	})
	kongContext.FatalIfErrorf(err)
}

func getLogger(debug bool) zerolog.Logger {
	logLevel := zerolog.InfoLevel
	if debug {
		logLevel = zerolog.DebugLevel
	}

	return zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		Level(logLevel).With().Timestamp().Logger()
}

func (cmdProducer *runProducerCmd) Run(cmdContext *commandContext) error {
	cmdContext.logger.Info().
		Int("Inserts", cmdProducer.Inserts).
		Int("Updates", cmdProducer.Updates).
		Msg("running producer")

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cmdProducer.ConnectionString))
	if err != nil {
		cmdContext.logger.Fatal().Err(err).Msg("error connecting to the database")
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			cmdContext.logger.Fatal().Err(err).Msg("error closing connection")
		}
	}()

	people := client.Database(databaseName).Collection(collectionName)

	// Insert new records
	for i := 0; i < cmdProducer.Inserts; i++ {
		time.Sleep(time.Duration(cmdProducer.DelayBetweenInserts) * time.Millisecond)
		p := &person{
			FirstName: fmt.Sprintf("FirstName%v", i+1),
			LastName:  "LastName",
			Created:   time.Now(),
			Modified:  time.Now(),
		}
		result, err := people.InsertOne(ctx, p)
		if err != nil {
			cmdContext.logger.Fatal().Err(err).Msg("error inserting records")
		}
		cmdContext.logger.Info().Interface("InsertedID", result.InsertedID).Msg("record inserted")

		// Perform updates on inserted record
		for j := 0; j < cmdProducer.Updates; j++ {
			// Pause for a bit
			time.Sleep(time.Duration(cmdProducer.DelayBetweenUpdates) * time.Millisecond)

			// Patch lastname and modified fields
			fields := bson.D{}
			fields = append(fields,
				bson.E{Key: "lastname", Value: fmt.Sprintf("LastName%v", j+1)},
				bson.E{Key: "modified", Value: time.Now()},
			)
			patch := bson.D{
				bson.E{Key: "$set", Value: fields},
			}

			// Find record to update by _id
			filter := bson.D{bson.E{Key: "_id", Value: result.InsertedID}}

			// Do the update
			var updatedPerson *person
			err = people.FindOneAndUpdate(ctx, filter, patch, options.FindOneAndUpdate().SetReturnDocument(options.After)).Decode(&updatedPerson)
			if err != nil {
				cmdContext.logger.Fatal().Err(err).Msg("error performing update")
			}

			cmdContext.logger.Info().Int("count", j+1).Str("LastName", updatedPerson.LastName).Msg("record updated")
		}
	}

	return nil
}

func (cmdConsumer *runConsumerCmd) Run(cmdContext *commandContext) error {
	cmdContext.logger.Info().Msg("running consumer")

	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(cmdConsumer.ConnectionString))
	if err != nil {
		cmdContext.logger.Fatal().Err(err).Msg("error connecting to the database")
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			cmdContext.logger.Fatal().Err(err).Msg("error closing connection")
		}
	}()

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	changeStream, err := client.Database(databaseName).Collection(collectionName).Watch(ctx, mongo.Pipeline{}, opts)
	if err != nil {
		cmdContext.logger.Fatal().Err(err).Msg("error getting DB change-stream")
	}
	defer func() {
		err = changeStream.Close(ctx)
		if err != nil {
			cmdContext.logger.Fatal().Err(err).Msg("error closing change stream")
		}
	}()

	cmdContext.logger.Info().Msg("waiting for changes")

	for changeStream.Next(ctx) {
		record := map[string]interface{}{}
		err = bson.Unmarshal(changeStream.Current, record)
		if err != nil {
			cmdContext.logger.Fatal().Err(err).Msg("error decoding change-stream record")
		}
		cmdContext.logger.Info().Interface("record", record).Msg("change stream record")
	}

	return nil
}
