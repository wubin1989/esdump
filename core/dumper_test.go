package core_test

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/unionj-cloud/go-doudou/test"
	"github.com/unionj-cloud/go-doudou/toolkit/constants"
	"github.com/wubin1989/esdump/core"
	"github.com/wubin1989/go-esutils"
	"os"
	"testing"
	"time"
)

var esAddr, input string

func TestMain(m *testing.M) {
	os.Setenv("TZ", "Asia/Shanghai")
	terminator, esHost, esPort := test.PrepareTestEnvironment()
	esAddr = fmt.Sprintf("http://%s:%d", esHost, esPort)
	esIndex := "test"
	input = esAddr + "/" + esIndex
	es := esutils.NewEs(esIndex, esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	prepareTestIndex(es)
	prepareTestData(es)
	code := m.Run()
	terminator()
	os.Exit(code)
}

func prepareTestIndex(es *esutils.Es) {
	mapping := esutils.NewMapping(esutils.MappingPayload{
		esutils.Base{
			Index: es.GetIndex(),
			Type:  es.GetType(),
		},
		[]esutils.Field{
			{
				Name: "createAt",
				Type: esutils.DATE,
			},
			{
				Name: "text",
				Type: esutils.TEXT,
			},
		},
	})
	_, err := es.NewIndex(context.Background(), mapping)
	if err != nil {
		panic(err)
	}
}

func prepareTestData(es *esutils.Es) {
	data1 := "2020-06-01"
	data2 := "2020-06-20"
	data3 := "2020-07-10"

	createAt1, _ := time.ParseInLocation(constants.FORMAT2, data1, time.Local)
	createAt2, _ := time.ParseInLocation(constants.FORMAT2, data2, time.Local)
	createAt3, _ := time.ParseInLocation(constants.FORMAT2, data3, time.Local)

	err := es.BulkSaveOrUpdate(context.Background(), []interface{}{
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh5",
			"createAt": createAt1.UTC().Format(constants.FORMATES),
			"type":     "education",
			"text":     "2020年7月8日11时25分，高考文科综合/理科综合科目考试将要结束时，平顶山市一中考点一考生突然情绪失控，先后抓其右边、后边考生答题卡，造成两位考生答题卡损毁。",
		},
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh6",
			"createAt": createAt2.UTC().Format(constants.FORMATES),
			"type":     "sport",
			"text":     "考场两位监考教师及时制止，并稳定了考场秩序，市一中考点按程序启用备用答题卡，按规定补足答题卡被损毁的两位考生耽误的考试时间，两位考生将损毁卡的内容誊写在新答题卡上。",
		},
		map[string]interface{}{
			"id":       "9seTXHoBNx091WJ2QCh7",
			"createAt": createAt3.UTC().Format(constants.FORMATES),
			"type":     "culture",
			"text":     "目前，我办已将损毁其他考生答题卡的考生违规情况上报河南省招生办公室，将依规对该考生进行处理。平顶山市招生考试委员会办公室",
		},
	})
	if err != nil {
		panic(err)
	}
}

func TestDumper_DumpMapping(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpmapping"
	dumper := core.NewDumper(core.Config{
		Input:    input,
		Output:   esAddr + "/" + esIndex,
		DumpType: "mapping",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.GetMapping(ctx)
	assert.NoError(t, err)
	assert.NotZero(t, ret)
}

func TestDumper_DumpData(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdata"
	dumper := core.NewDumper(core.Config{
		Input:     input,
		Output:    esAddr + "/" + esIndex,
		DumpType:  "data",
		DateField: "createAt",
		StartDate: "2020-06-01",
		EndDate:   "",
		Step:      240 * time.Hour,
		Zone:      "UTC",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, int(ret))
}

func TestDumper_DumpAll(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpall"
	dumper := core.NewDumper(core.Config{
		Input:     input,
		Output:    esAddr + "/" + esIndex,
		DateField: "createAt",
		StartDate: "2020-06-01",
		EndDate:   "",
		Step:      240 * time.Hour,
		Zone:      "UTC",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, int(ret))
}
