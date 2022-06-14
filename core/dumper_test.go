package core_test

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/unionj-cloud/go-doudou/toolkit/constants"
	"github.com/wubin1989/esdump/v2/core"
	"github.com/wubin1989/go-esutils/v2"
	"os"
	"testing"
	"time"
)

// PrepareTestEnvironment prepares test environment
func PrepareTestEnvironment() (func(), string, int) {
	var terminateContainer func() // variable to store function to terminate container
	var host string
	var port int
	var err error
	terminateContainer, host, port, err = SetupEs6Container(logrus.New())
	if err != nil {
		panic("failed to setup Elasticsearch container")
	}
	return terminateContainer, host, port
}

// SetupEs6Container starts elasticsearch 6.8.12 docker container
func SetupEs6Container(logger *logrus.Logger) (func(), string, int, error) {
	logger.Info("setup Elasticsearch v6 Container")
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "elasticsearch:6.8.12",
		ExposedPorts: []string{"9200/tcp", "9300/tcp"},
		Env: map[string]string{
			"discovery.type": "single-node",
		},
		WaitingFor: wait.ForLog("started"),
	}

	esC, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})

	if err != nil {
		logger.Errorf("error starting Elasticsearch container: %s", err)
		panic(fmt.Sprintf("%v", err))
	}

	closeContainer := func() {
		logger.Info("terminating container")
		err := esC.Terminate(ctx)
		if err != nil {
			logger.Errorf("error terminating Elasticsearch container: %s", err)
			panic(fmt.Sprintf("%v", err))
		}
	}

	host, _ := esC.Host(ctx)
	p, _ := esC.MappedPort(ctx, "9200/tcp")
	port := p.Int()

	return closeContainer, host, port, nil
}

var esAddr, input string

func TestMain(m *testing.M) {
	os.Setenv("TZ", "Asia/Shanghai")
	terminator, esHost, esPort := PrepareTestEnvironment()
	esAddr = fmt.Sprintf("http://%s:%d", esHost, esPort)
	esIndex := "test"
	input = esAddr + "/" + esIndex
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
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
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
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
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, int(ret))
}

func TestDumper_DumpDataDesc(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdatadesc"
	dumper := core.NewDumper(core.Config{
		Input:      input,
		Output:     esAddr + "/" + esIndex,
		DumpType:   "data",
		DateField:  "createAt",
		StartDate:  "2020-06-01",
		EndDate:    "",
		Step:       240 * time.Hour,
		Zone:       "UTC",
		Descending: true,
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, int(ret))
}

func TestDumper_DumpData2(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdata2"
	dumper := core.NewDumper(core.Config{
		Input:     input,
		Output:    esAddr + "/" + esIndex,
		DumpType:  "data",
		DateField: "createAt",
		StartDate: "2020-06-01",
		EndDate:   "2020-07-01",
		Step:      240 * time.Hour,
		Zone:      "UTC",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, int(ret))
}

func TestDumper_DumpDataIncludes(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdataincludes"
	dumper := core.NewDumper(core.Config{
		Input:     input,
		Output:    esAddr + "/" + esIndex,
		DumpType:  "data",
		DateField: "createAt",
		StartDate: "2020-06-01",
		EndDate:   "2020-07-01",
		Step:      240 * time.Hour,
		Zone:      "UTC",
		Includes:  "id,text",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, int(ret))
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := es.List(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	doc := list[0].(map[string]interface{})
	_, ok := doc["type"]
	assert.False(t, ok)
	_, ok = doc["createAt"]
	assert.False(t, ok)
	_, ok = doc["text"]
	assert.True(t, ok)
	_, ok = doc["id"]
	assert.True(t, ok)
}

func TestDumper_DumpDataExcludes(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdataexcludes"
	dumper := core.NewDumper(core.Config{
		Input:     input,
		Output:    esAddr + "/" + esIndex,
		DumpType:  "data",
		DateField: "createAt",
		StartDate: "2020-06-01",
		EndDate:   "2020-07-01",
		Step:      240 * time.Hour,
		Zone:      "UTC",
		Excludes:  "text",
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, int(ret))
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := es.List(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	doc := list[0].(map[string]interface{})
	_, ok := doc["type"]
	assert.True(t, ok)
	_, ok = doc["createAt"]
	assert.True(t, ok)
	_, ok = doc["text"]
	assert.False(t, ok)
	_, ok = doc["id"]
	assert.True(t, ok)
}

func TestDumper_DumpDataIncludesDesc(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdataincludesdesc"
	dumper := core.NewDumper(core.Config{
		Input:      input,
		Output:     esAddr + "/" + esIndex,
		DumpType:   "data",
		DateField:  "createAt",
		StartDate:  "2020-06-01",
		EndDate:    "2020-07-01",
		Step:       240 * time.Hour,
		Zone:       "UTC",
		Includes:   "id,text",
		Descending: true,
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, int(ret))
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := es.List(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	doc := list[0].(map[string]interface{})
	_, ok := doc["type"]
	assert.False(t, ok)
	_, ok = doc["createAt"]
	assert.False(t, ok)
	_, ok = doc["text"]
	assert.True(t, ok)
	_, ok = doc["id"]
	assert.True(t, ok)
}

func TestDumper_DumpDataExcludesDesc(t *testing.T) {
	t.Parallel()
	esIndex := "test_dumpdataexcludesdesc"
	dumper := core.NewDumper(core.Config{
		Input:      input,
		Output:     esAddr + "/" + esIndex,
		DumpType:   "data",
		DateField:  "createAt",
		StartDate:  "2020-06-01",
		EndDate:    "2020-07-01",
		Step:       240 * time.Hour,
		Zone:       "UTC",
		Excludes:   "text",
		Descending: true,
	})
	dumper.Dump()
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, int(ret))
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	list, err := es.List(ctx, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(list))
	doc := list[0].(map[string]interface{})
	_, ok := doc["type"]
	assert.True(t, ok)
	_, ok = doc["createAt"]
	assert.True(t, ok)
	_, ok = doc["text"]
	assert.False(t, ok)
	_, ok = doc["id"]
	assert.True(t, ok)
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
	es := esutils.NewEs(esIndex, esutils.WithLogger(logrus.StandardLogger()), esutils.WithUrls([]string{esAddr}))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ret, err := es.Count(ctx, nil)
	assert.NoError(t, err)
	assert.Equal(t, 3, int(ret))
}
