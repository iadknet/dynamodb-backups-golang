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

	log "github.com/sirupsen/logrus"
)

type Config struct {
	TableRegex       string `env:"TABLE_REGEX"`
	BackupExpireDays int    `env:"BACKUP_EXPIRE_DAYS" envDefault:"1"`
	LogLevel         string `env:"LOG_LEVEL" envDefault:"info"`
	LogFormatter     string `env:"LOG_FORMATTER" envDefault:"text"`
}

var config = Config{}
var dynamo = &dynamodb.DynamoDB{}

func init() {

	// parse configuration
	env.Parse(&config)

	// initialize dynamo client
	dynamo = dynamodb.New(session.New())

	// Output to stdout
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	logLevel, err := log.ParseLevel(config.LogLevel)
	if err != nil {
		log.SetLevel(log.InfoLevel)
		log.Error("Could not read log level from configuration, defaulting to INFO")
	}
	log.SetLevel(logLevel)

	// set the log formatter
	if config.LogFormatter == "text" {
		log.SetFormatter(&log.TextFormatter{})
	} else if config.LogFormatter == "json" {
		log.SetFormatter(&log.JSONFormatter{})
	} else {
		log.SetFormatter(&log.JSONFormatter{})
		log.Error("Could not read log formatter from configuration, defaulting to JSON")
	}
}

func main() {

	matchedTables := getTablesRegex(config.TableRegex)

	log.Debug("Matched tables", matchedTables)

	for _, table := range matchedTables {
		createBackup(table)
		expireBackups(table)
	}

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

func createBackup(table string) {
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

	if err == nil { // resp is now filled
		log.Debug("Creating backup: ", resp)
	} else {
		log.Error(err)
	}

}

func expireBackups(table string) {

	timeRangeUpperBound := time.Now().AddDate(0, 0, -config.BackupExpireDays)

	listBackupsInput := dynamodb.ListBackupsInput{
		TableName:           &table,
		TimeRangeUpperBound: &timeRangeUpperBound,
	}

	listBackupsOutput, err := dynamo.ListBackups(&listBackupsInput)
	log.Debug("listBackupsOutput: ", listBackupsOutput)
	if err == nil {
		for _, backupSummary := range listBackupsOutput.BackupSummaries {
			deleteBackup(backupSummary)
		}
	} else {
		log.Error(err)
	}

}

func deleteBackup(backupSummary *dynamodb.BackupSummary) {

	deleteBackupInput := dynamodb.DeleteBackupInput{
		BackupArn: backupSummary.BackupArn,
	}

	log.Info("Deleting backup: ", deleteBackupInput)
	deleteBackupOutput, err := dynamo.DeleteBackup(&deleteBackupInput)

	if err == nil {
		log.Debug(deleteBackupOutput)
	} else {
		log.Error(err)
	}

}
