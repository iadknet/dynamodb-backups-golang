package main

import (
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/caarlos0/env"
	"github.com/onrik/logrus/filename"
	"github.com/sirupsen/logrus"
)

type Config struct {
	TableRegex       string `env:"TABLE_REGEX"`
	BackupExpireDays int    `env:"BACKUP_EXPIRE_DAYS" envDefault:"1"`
	LogLevel         string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormatter     string `env:"LOG_FORMATTER" envDefault:"text"`
}

// ExpireMessage Struct for messages sent over the expire channel
type ExpireMessage struct {
	TableName string
	Count     int
	Error     error
}

// CreateMessage Struct for messages sent over the create channel
type CreateMessage struct {
	TableName  string
	BackupName string
	Error      error
}

// DeleteMessage Struct for messages sent over the delete channel
type DeleteMessage struct {
	TableName  string
	BackupName string
	Error      error
}

var config = &Config{}
var dynamo = &dynamodb.DynamoDB{}
var log = &logrus.Entry{}

func init() {

	// parse configuration
	env.Parse(config)

	// initialize dynamo client
	dynamo = dynamodb.New(session.New())

	// Output to stdout
	logrus.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logLevel, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.SetLevel(logrus.InfoLevel)
		logrus.Error("Could not read log level from configuration, defaulting to INFO")
	}
	logrus.SetLevel(logLevel)

	// set the log formatter
	if config.LogFormatter == "text" {
		logrus.SetFormatter(&logrus.TextFormatter{})
	} else if config.LogFormatter == "json" {
		logrus.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logrus.SetFormatter(&logrus.JSONFormatter{})
		logrus.Error("Could not read log formatter from configuration, defaulting to JSON")
	}

	// Add filename and line number if logging level is debug
	if logLevel == logrus.DebugLevel {
		logrus.AddHook(filename.NewHook())
	}

	// Add common context to log messages
	log = logrus.WithFields(
		logrus.Fields{
			"service": "dynamodb-backups",
		},
	)
}

func main() {
	start := time.Now()

	matchedTables := getTablesRegex(config.TableRegex)
	tableCount := len(matchedTables)

	log.WithFields(logrus.Fields{
		"matchedTablesList": matchedTables,
		"count":             tableCount,
		"regex":             config.TableRegex,
	}).Info(fmt.Sprintf("Matched %d tables", len(matchedTables)))

	createChannel := make(chan CreateMessage, tableCount)
	expireChannel := make(chan ExpireMessage, tableCount)

	for _, table := range matchedTables {

		go createBackup(table, createChannel)
		go expireBackups(table, expireChannel)
	}

	for i := 0; i < tableCount; i++ {
		createMessage := <-createChannel
		tableName := createMessage.TableName
		backupName := createMessage.BackupName
		log.WithFields(logrus.Fields{
			"table":      tableName,
			"backupName": backupName,
		}).Info(fmt.Sprintf("Created backup for table %s", tableName))
	}

	for i := 0; i < tableCount; i++ {
		expireMessage := <-expireChannel
		tableName := expireMessage.TableName
		deletedCount := expireMessage.Count
		log.WithFields(logrus.Fields{
			"table": tableName,
			"count": deletedCount,
		}).Info(fmt.Sprintf("Deleted %d backups from table %s", deletedCount, tableName))
	}

	elapsed := time.Since(start)

	log.Info(fmt.Sprintf("Main() execution time: %s", elapsed))
}

func getTablesRegex(pattern string) []string {

	matchedTables := make([]string, 0)
	patternRegex, _ := regexp.Compile(pattern)

	pageNum := 0
	input := &dynamodb.ListTablesInput{}
	err := dynamo.ListTablesPages(input,
		func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
			pageNum++
			for _, name := range page.TableNames {
				if patternRegex.MatchString(*name) {
					matchedTables = append(matchedTables, *name)
				}
			}
			return lastPage
		})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case dynamodb.ErrCodeInternalServerError:
				log.Error(dynamodb.ErrCodeInternalServerError, aerr.Error())
			default:
				log.Error(aerr.Error())
			}
		}
	}

	return matchedTables
}

func createBackup(table string, createChannel chan CreateMessage) {

	localLogger := log.WithFields(logrus.Fields{
		"table": table,
	})

	now := time.Now().UTC()

	timestamp := fmt.Sprintf("%d%02d%02d%02d%02d",
		now.Year(), now.Month(), now.Day(),
		now.Minute(), now.Second(),
	)

	backupName := fmt.Sprintf("%s_%s", table, timestamp)

	params := dynamodb.CreateBackupInput{
		BackupName: &backupName,
		TableName:  &table,
	}

	resp, err := dynamo.CreateBackup(&params)

	// need to figure out how to pass errors back to the channel
	if err == nil {

		localLogger.WithFields(logrus.Fields{
			"action":     "createBackup",
			"BackupName": backupName,
		}).Info(fmt.Sprintf("Creating backup for table %s", table))

		localLogger.WithFields(logrus.Fields{
			"responseObject": resp,
		}).Debug("Creating backup")

		createChannel <- CreateMessage{
			TableName:  table,
			BackupName: backupName,
		}
	} else {
		localLogger.Error(err)
		createChannel <- CreateMessage{
			TableName:  table,
			BackupName: backupName,
			Error:      err,
		}
	}

}

func expireBackups(table string, expireChannel chan ExpireMessage) {

	localLogger := log.WithFields(logrus.Fields{
		"table": table,
	})

	timeRangeUpperBound := time.Now().AddDate(0, 0, -config.BackupExpireDays)

	listBackupsInput := dynamodb.ListBackupsInput{
		TableName:           &table,
		TimeRangeUpperBound: &timeRangeUpperBound,
	}

	listBackupsOutput, err := dynamo.ListBackups(&listBackupsInput)
	localLogger.WithFields(logrus.Fields{
		"listBackupsOutput": listBackupsOutput,
	}).Debug("listBackupsOutput")

	deleteCount := len(listBackupsOutput.BackupSummaries)
	deleteChannel := make(chan string, deleteCount)
	if err == nil {
		for _, backupSummary := range listBackupsOutput.BackupSummaries {
			go deleteBackup(backupSummary, deleteChannel)
		}
	} else {
		close(deleteChannel)
		localLogger.Error(err)
	}

	for i := 0; i < deleteCount; i++ {
		<-deleteChannel
	}

	expireChannel <- ExpireMessage{
		TableName: table,
		Count:     deleteCount,
	}
}

func deleteBackup(backupSummary *dynamodb.BackupSummary, deleteChannel chan string) {
	localLogger := log.WithFields(logrus.Fields{
		"backupName": *backupSummary.BackupName,
		"table":      *backupSummary.TableName,
		"action":     "deleteBackup",
	})

	deleteBackupInput := dynamodb.DeleteBackupInput{
		BackupArn: backupSummary.BackupArn,
	}

	localLogger.WithFields(logrus.Fields{
		"deleteBackupInput": deleteBackupInput,
	}).Debug("deleteBackupInput")

	localLogger.Info(fmt.Sprintf("Deleting backup for table %s", *backupSummary.TableName))
	deleteBackupOutput, err := dynamo.DeleteBackup(&deleteBackupInput)

	if err == nil {
		deleteChannel <- *deleteBackupOutput.BackupDescription.BackupDetails.BackupName
		localLogger.WithFields(logrus.Fields{
			"deleteBackupOutput": deleteBackupOutput,
		}).Debug("deleteBackupOutput")

	} else {
		deleteChannel <- err.Error()
		localLogger.Error(err)
	}
}
