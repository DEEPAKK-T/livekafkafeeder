# LiveKafkaFeeder: Real-time Log Tailing and Publishing to Kafka

## Overview

Watches a directory for new log files.
Reads log files in real-time, starting from the last position.
Sends log messages to Kafka topics.
Manages position files to track progress across restarts and ensures that no duplicate logs are published to Kafka.
## Features

Efficient tailing with fsnotify for efficient file system event monitoring.
Kafka integration for reliable log delivery.
Customizable settings for Kafka brokers, topics, and position file location.
Cleanup of old position files.
## Installation

```go
go get -u github.com/DEEPAKK-T/livekafkafeeder
```
## Usage


```go
package main

import (
	"fmt"
	"time"

	"github.com/DEEPAKK-T/livekafkafeeder"
)

func main() {

	// Kafka broker addresses (comma-separated)
	brokerAddresses := "kafka1:9092,kafka2:9092"
	// Kafka topic to publish logs to
	kafkaTopic := "my-logs"
	// Directory containing log files
	sourceLogsDir := "/path/to/logs"
	// Directory for position files
	positionFilesDir := "/path/to/position-files"
	// Interval for cleaning up old position files
	cleanupInterval := 24 * time.Hour

	err := livekafkafeeder.LiveWatchKafkaFeeder(
		brokerAddresses,
		kafkaTopic,
		sourceLogsDir,
		positionFilesDir,
		cleanupInterval)
	if err != nil {
		fmt.Printf("Error in LiveWatchKafkaFeeder %v", err)
	}

}
