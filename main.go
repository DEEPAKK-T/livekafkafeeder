package livekafkafeeder

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/fsnotify/fsnotify"
)

var wg sync.WaitGroup

func LiveWatchKafkaFeeder(kafkaBrokerAddresses, kafkaTopic, sourceLogsDir, positionFilesDir string, cleanupOldPositionFilesInterval time.Duration) error {

	// Kafka producer configuration.
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true                  // Set this to true when using a SyncProducer.
	config.Producer.RequiredAcks = sarama.WaitForLocal       // Only wait for the leader to acknowledge the message.
	config.Producer.Compression = sarama.CompressionSnappy   // Compress messages for efficiency.
	config.Producer.Flush.Frequency = 500 * time.Millisecond // Flush batches every 500ms.

	fmt.Println("Starting Kafka producer")

	// Create a new Kafka producer.
	producer, err := sarama.NewSyncProducer(strings.Split(kafkaBrokerAddresses, ","), config) //sarama.NewSyncProducer(strings.Split(brokerAddresses, ","), config)
	if err != nil {
		return fmt.Errorf("error creating Kafka producer: %v", err)
	}
	defer producer.Close()

	// Process the latest logs when the program starts
	err = processLatestLogs(sourceLogsDir, kafkaTopic, producer, positionFilesDir)
	if err != nil {
		return fmt.Errorf("error processing latest logs: %v", err)
	}

	go cleanupOldPositionFiles(cleanupOldPositionFilesInterval, positionFilesDir)

	// Create a new watcher
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// Add the logs directory to the watcher
	err = watcher.Add(sourceLogsDir)
	if err != nil {
		return err
	}

	// Run a background goroutine to continuously process logs.
	go func() {

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Op&fsnotify.Create == fsnotify.Create {
					// Check if the event is a create operation
					if filepath.Ext(event.Name) == ".log" {
						wg.Add(1)
						// Watch for new events in the log file continuously
						fmt.Printf("Started watching logs for: \n")
						go func(file string) {
							defer wg.Done()
							if err := watchLogFile(event.Name, kafkaTopic, producer, positionFilesDir); err != nil {
								log.Fatal(err)
							}
						}(event.Name)

						fmt.Println(event.Name)
						if err != nil {
							log.Printf("Error processing log file %s: %v", event.Name, err)
						}

					}
				}

			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("Error watching directory: %v", err)
			}
		}
	}()

	select {}

}

func watchLogFile(logFilePath string, topic string, producer sarama.SyncProducer, positionFilesDir string) error {
	defer wg.Done() //Remove if not required
	// Create a new watcher
	fileWatcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer fileWatcher.Close()
	// Increase the buffer size to 10000
	fileWatcher.Events = make(chan fsnotify.Event, 10000)

	// Add the logs directory to the watcher
	err = fileWatcher.Add(logFilePath)
	if err != nil {
		return err
	}

	for {
		select {
		case event, ok := <-fileWatcher.Events:
			if !ok {
				return nil
			}

			// Handle new events in the log file (write events)
			if event.Op&fsnotify.Write == fsnotify.Write {
				// Process the new content in the log file
				err := processNewContent(logFilePath, topic, producer, positionFilesDir)
				if err != nil {
					return fmt.Errorf("error processing new content in log file %s: %v", logFilePath, err)
				}
			}

			// Check if the file was removed
			if event.Op&fsnotify.Remove == fsnotify.Remove && event.Name == logFilePath {
				// File has been deleted, remove it from the watcher and exit the function
				fileWatcher.Remove(logFilePath)
				fmt.Printf("Removed watcher for the file: %s\n", logFilePath)
				return nil
			}

		case err, ok := <-fileWatcher.Errors:
			if !ok {
				return nil
			}
			log.Printf("Error watching log file: %v", err)
		}
	}
}

func processNewContent(logFilePath string, topic string, producer sarama.SyncProducer, positionFilesDir string) error {

	// Check if the file exists before opening it
	if _, err := os.Stat(logFilePath); os.IsNotExist(err) {
		return nil
	}

	// Open the log file
	file, err := os.Open(logFilePath)
	if err != nil {
		return fmt.Errorf("file not found, Logs are already pushed and deleted the file %s: %v", logFilePath, err)
	}
	defer file.Close()

	// Seek to the last processed position
	// This is where you'd need to retrieve the last processed position from your storage
	// or wherever it is being tracked.
	lastProcessedPosition, err := getLastProcessedPosition(logFilePath, positionFilesDir)
	if err != nil {
		return fmt.Errorf("error getting last processed position for log file %s: %v", logFilePath, err)
	}

	//moving read cursor to a lastProcessedPosition within the file.
	_, err = file.Seek(lastProcessedPosition, io.SeekStart)
	if err != nil {
		return fmt.Errorf("error seeking to last processed position in log file %s: %v", logFilePath, err)
	}

	// Read new content from the log file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Process the new line - send it to Kafka
		message := &sarama.ProducerMessage{Topic: topic, Value: sarama.StringEncoder(line)}
		_, _, err := producer.SendMessage(message)
		if err != nil {
			log.Printf("Failed to send message to Kafka: %v", err)
			return fmt.Errorf("error producing message to Kafka: %v", err)
		} else {
			fmt.Printf("Message sent to Kafka: %s\n", line)
		}

		// Update the last processed position
		lastProcessedPosition, err = file.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("error updating last processed position in log file %s: %v", logFilePath, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading log file %s: %v", logFilePath, err)
	}

	// Save the last processed position for the next iteration
	err = updateLastProcessedPosition(logFilePath, lastProcessedPosition, positionFilesDir)
	if err != nil {
		return fmt.Errorf("error updating last processed position in storage for log file %s: %v", logFilePath, err)
	}

	return nil
}

func getLastProcessedPosition(logFilePath, positionFilesDir string) (int64, error) {
	// Create a filename for storing the last processed position
	positionFilePath := getPositionFilePath(logFilePath, positionFilesDir)

	// Read the last processed position from the file
	content, err := ioutil.ReadFile(positionFilePath)
	if err != nil {
		// If the file doesn't exist, return 0 as the default position
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("error reading last processed position from file %s: %v", positionFilePath, err)
	}

	strcontent := strings.TrimSpace(string(content))
	if strcontent == "" {
		// If the content is empty, return 0 as the default position
		return 0, nil
	}

	// Parse the strcontent as an integer
	lastPosition, err := strconv.ParseInt(strcontent, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing last processed position from file %s: %v", positionFilePath, err)
	}

	return lastPosition, nil
}

func updateLastProcessedPosition(logFilePath string, position int64, positionFilesDir string) error {
	// Create a filename for storing the last processed position
	positionFilePath := getPositionFilePath(logFilePath, positionFilesDir)

	// Write the updated position to the file
	err := ioutil.WriteFile(positionFilePath, []byte(fmt.Sprintf("%d", position)), 0644)
	if err != nil {
		return fmt.Errorf("error updating last processed position in file %s: %v", positionFilePath, err)
	}

	return nil
}

func getPositionFilePath(logFilePath, positionFilesDir string) string {

	// Create the "/position" directory if it doesn't exist
	if err := os.MkdirAll(positionFilesDir, os.ModePerm); err != nil {
		fmt.Printf("Error creating directory: %v\n", err)
		return ""
	}
	// Use the log file name to create a unique position file name
	return filepath.Join(positionFilesDir, fmt.Sprintf("%s.position", filepath.Base(logFilePath)))
}

func cleanupOldPositionFiles(timeInternal time.Duration, positionFilesDir string) {
	for {
		// Sleep for the cleanup interval
		time.Sleep(timeInternal) //12 * time.Hour

		// Get the list of position files in the directory
		positionFiles, err := ioutil.ReadDir(positionFilesDir)
		if err != nil {
			log.Printf("Error reading position directory: %v", err)
			continue
		}

		// Determine the current time
		currentTime := time.Now()

		// Delete position files older than 3 hours
		for _, file := range positionFiles {
			if file.ModTime().Before(currentTime.Add(-3 * time.Hour)) { //-12 * time.Hour
				positionFilePath := filepath.Join(positionFilesDir, file.Name())
				err := os.Remove(positionFilePath)
				if err != nil {
					log.Printf("Error deleting old position file %s: %v", positionFilePath, err)
				} else {
					log.Printf("Deleted old position file: %s", positionFilePath)
				}
			}
		}
	}
}

func processLatestLogs(directory string, topic string, producer sarama.SyncProducer, positionFilesDir string) error {
	// Get a list of all log files in the directory
	files, err := filepath.Glob(filepath.Join(directory, "*.log"))
	if err != nil {
		return err
	}

	// Sort files by modification time in descending order
	sort.Slice(files, func(i, j int) bool {
		fi, _ := os.Stat(files[i])
		fj, _ := os.Stat(files[j])
		return fi.ModTime().After(fj.ModTime())
	})

	// Process the latest log files
	for _, file := range files {
		wg.Add(1)
		fmt.Printf("Started watching logs for Initial file: %s\n", file)
		go func(file string) {
			defer wg.Done()
			if err := watchLogFile(file, topic, producer, positionFilesDir); err != nil {
				log.Fatal(err)
			}
		}(file)
	}

	return nil
}
