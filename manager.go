package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
)

type Manager struct {
	Worker       *OffsetWorker
	PusherGeters []*PusherGeter
	Zookeepers   []string
	ZkCluster    string

	Logger_file   string
	Monitor_file  string
	Err_file      string
	Logger_switch int

	Distance int
	Passby   []Clusterlist
}

func NewManager(monitor string, logger string, switcher int, errfile string) *Manager {
	return &Manager{Monitor_file: monitor, Logger_file: logger, Logger_switch: switcher, Err_file: errfile}
}

func (this *Manager) Init(config *Config) error {
	this.Zookeepers = config.Zookeepers
	this.ZkCluster = config.ZkCluster

	this.Distance = config.Distance
	this.Passby = config.Passby

	this.Worker = NewOffsetWorker(this.Zookeepers[0], this.ZkCluster, this.Err_file)
	err := this.Worker.Init()
	if err != nil {
		AddLogger(this.Err_file, "[Distance Err MAN_INIT_ERR]", err)
		return err
	}

	return nil
}

func (this *Manager) Work() error {
	// kafka get data from brokerList
	host, err := os.Hostname()

	// pass_by
	passBy := map[string]map[string]string{}
	for _, value := range this.Passby {
		item := map[string]string{}
		item[value.ConsumerGroup] = value.Topic
		passBy[value.Cluster] = item
	}

	var data []LogData
	kafkaOffset, err := this.Worker.GetLastOffset()
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err MAN_KAFKA_WORKER_ERR]", err)
		return err
	}
	zkOffset, err := this.Worker.GetConsumerGroupsOffsetDistance()
	if nil != err {
		AddLogger(this.Err_file, "[Distance Err MAN_ZK_WORKER_ERR]", err)
		return err
	}

	topicKeyList := []string{}
	for topicKey, v := range kafkaOffset {
		topicKeyList = append(topicKeyList, topicKey)
		data = append(data, LogData{
			Host:          host,
			Zabbix_key:    ZABBIX_KEY_LASTEST_OFFSET,
			Cluster:       this.ZkCluster,
			ConsumerGroup: "na",
			Url:           "na",
			Topic:         topicKey,
			Threshold:     INT64_MAX,
			Distance:      v["total"],
		})
	}

	msgLog := []string{}
	for consumergroup, group := range zkOffset {
		for topic, topicData := range group {
			passbytopic, ok := passBy[this.ZkCluster][consumergroup]
			if ok && passbytopic == topic {
				continue
			}
			for partition, offset := range topicData {
				if partition == "total" {
					data = append(data, LogData{
						Host:          host,
						Zabbix_key:    ZABBIX_KEY_DISTANCE,
						Cluster:       this.ZkCluster,
						ConsumerGroup: consumergroup,
						Url:           "-",
						Topic:         topic,
						Threshold:     this.Distance,
						Distance:      offset,
					})
					continue
				}
				s := fmt.Sprintf("[Distance Data] topic:%v cg:%v url:- partition:%v distance:%d", topic, consumergroup, partition, offset)
				msgLog = append(msgLog, s)
			}
		}
	}

	if this.Logger_switch == 1 {
		logger := NewFileLogger(this.Logger_file, msgLog)
		logger.RecordLogger()
	}

	writer := NewFileWriter(this.Monitor_file, data)
	writer.WriteToFile()

	return nil
}

func (this *Manager) Close() {
	this.Worker.Close()
}

func getGroupNameByUrl(url string) string {
	m := md5.New()
	m.Write([]byte(url))
	s := hex.EncodeToString(m.Sum(nil))
	return s
}

func AddLogger(logger_path string, err_type string, err error) {
	msgLog := []string{}
	s := fmt.Sprintf("%v, %v", err_type, err)
	msgLog = append(msgLog, s)
	logger := NewFileLogger(logger_path, msgLog)
	logger.RecordLogger()
}
