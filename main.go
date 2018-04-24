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

	matchedTables := getTablesRegex(config.TableRegex)

	log.WithFields(logrus.Fields{
		"matchedTablesList": matchedTables,
		"count":             len(matchedTables),
	}).Info(fmt.Sprintf("Matche %d tables", len(matchedTables)))

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

	if err == nil { // resp is now filled

		localLogger.WithFields(logrus.Fields{
			"action":     "createBackup",
			"BackupName": backupName,
		}).Info(fmt.Sprintf("Creating backup for table %s", table))

		localLogger.WithFields(logrus.Fields{
			"responseObject": resp,
		}).Debug("Creating backup")

	} else {
		localLogger.Error(err)
	}

}

func expireBackups(table string) {
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

	if err == nil {
		for _, backupSummary := range listBackupsOutput.BackupSummaries {
			deleteBackup(backupSummary)
		}
	} else {
		localLogger.Error(err)
	}

}

func deleteBackup(backupSummary *dynamodb.BackupSummary) {
	localLogger := log.WithFields(logrus.Fields{
		"backupName": backupSummary.BackupName,
	})

	deleteBackupInput := dynamodb.DeleteBackupInput{
		BackupArn: backupSummary.BackupArn,
	}

	localLogger.WithFields(logrus.Fields{
		"deleteBackupInput": deleteBackupInput,
	}).Debug("deleteBackupInput")

	localLogger.Info(fmt.Sprintf("Deleting backup %s", *backupSummary.BackupName))
	deleteBackupOutput, err := dynamo.DeleteBackup(&deleteBackupInput)

	if err == nil {
		localLogger.WithFields(logrus.Fields{
			"deleteBackupOutput": deleteBackupOutput,
		}).Debug("deleteBackupOutput")

	} else {
		localLogger.Error(err)
	}

}
