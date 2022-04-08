package core

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Jeffail/gabs/v2"
	"github.com/araddon/dateparse"
	"github.com/olivere/elastic"
	"github.com/schollz/progressbar/v3"
	"github.com/unionj-cloud/go-doudou/toolkit/constants"
	"github.com/unionj-cloud/go-doudou/toolkit/stringutils"
	"github.com/wubin1989/go-esutils"
	"net/url"
	"os"
	"strings"
	"time"
)

type Config struct {
	Input      string
	Output     string
	DumpType   string
	DateField  string
	StartDate  string
	EndDate    string
	Step       time.Duration
	ScrollSize int
	Descending bool
	Zone       string
	Includes   string
	Excludes   string
}

type Dumper struct {
	Conf         Config
	SourceClient *elastic.Client
	TargetClient *elastic.Client
	SourceIndex  string
	SourceType   string
	TargetIndex  string
	TargetType   string
	StartTime    *time.Time
	EndTime      *time.Time
	Zone         *time.Location
	Includes     []string `json:"includes"`
	Excludes     []string `json:"excludes"`
}

func NewDumper(conf Config) *Dumper {
	inputUrl, err := url.Parse(conf.Input)
	if err != nil {
		panic(err)
	}
	options := []elastic.ClientOptionFunc{
		elastic.SetURL([]string{fmt.Sprintf("%s://%s", inputUrl.Scheme, inputUrl.Host)}...),
		elastic.SetGzip(true),
	}
	username := inputUrl.User.Username()
	password, _ := inputUrl.User.Password()
	if stringutils.IsNotEmpty(username) {
		options = append(options, elastic.SetBasicAuth(username, password))
	}
	source, err := elastic.NewSimpleClient(options...)
	if err != nil {
		panic(err)
	}
	outputUrl, err := url.Parse(conf.Output)
	if err != nil {
		panic(err)
	}
	options = []elastic.ClientOptionFunc{
		elastic.SetURL([]string{fmt.Sprintf("%s://%s", outputUrl.Scheme, outputUrl.Host)}...),
		elastic.SetGzip(true),
	}
	username = outputUrl.User.Username()
	password, _ = outputUrl.User.Password()
	if stringutils.IsNotEmpty(username) {
		options = append(options, elastic.SetBasicAuth(username, password))
	}
	target, err := elastic.NewSimpleClient(options...)
	if err != nil {
		panic(err)
	}
	var (
		sourceIndex, sourceType, targetIndex, targetType string
	)

	sourcePath := strings.Split(strings.TrimSpace(strings.ReplaceAll(inputUrl.Path, "/", " ")), " ")
	if len(sourcePath) == 0 {
		panic("input index name should not be empty")
	}
	sourceIndex = sourcePath[0]
	if len(sourcePath) > 1 {
		sourceType = sourcePath[1]
	} else {
		sourceType = sourceIndex
	}

	targetPath := strings.Split(strings.TrimSpace(strings.ReplaceAll(outputUrl.Path, "/", " ")), " ")
	if len(targetPath) == 0 {
		panic("output index name should not be empty")
	}
	targetIndex = targetPath[0]
	if len(targetPath) > 1 {
		targetType = targetPath[1]
	} else {
		targetType = targetIndex
	}

	var startTime, endTime *time.Time
	if stringutils.IsNotEmpty(conf.StartDate) {
		start, err := time.ParseInLocation(constants.FORMAT2, conf.StartDate, time.Local)
		if err != nil {
			panic(err)
		}
		startTime = &start
	}

	if stringutils.IsNotEmpty(conf.EndDate) {
		end, err := time.ParseInLocation(constants.FORMAT2, conf.EndDate, time.Local)
		if err != nil {
			panic(err)
		}
		endTime = &end
	}

	var zone *time.Location
	if stringutils.IsNotEmpty(conf.Zone) {
		zone, err = time.LoadLocation(conf.Zone)
		if err != nil {
			panic(err)
		}
	} else {
		zone = time.Local
	}

	var includes, excludes []string
	if stringutils.IsNotEmpty(conf.Includes) {
		includes = strings.Split(conf.Includes, ",")
	}
	if stringutils.IsNotEmpty(conf.Excludes) {
		excludes = strings.Split(conf.Excludes, ",")
	}
	return &Dumper{
		Conf:         conf,
		SourceClient: source,
		TargetClient: target,
		SourceIndex:  sourceIndex,
		SourceType:   sourceType,
		TargetIndex:  targetIndex,
		TargetType:   targetType,
		StartTime:    startTime,
		EndTime:      endTime,
		Zone:         zone,
		Includes:     includes,
		Excludes:     excludes,
	}
}

func (d *Dumper) Dump() {
	switch d.Conf.DumpType {
	case "mapping":
		d.dumpMapping()
	case "data":
		d.dumpData()
	default:
		d.dumpMapping()
		d.dumpData()
	}
}

func (d *Dumper) dumpMapping() {
	sourceEs := esutils.NewEs(d.SourceIndex, d.SourceType, esutils.WithClient(d.SourceClient))
	targetEs := esutils.NewEs(d.TargetIndex, d.TargetType, esutils.WithClient(d.TargetClient))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mapping, err := sourceEs.GetMapping(ctx)
	if err != nil {
		panic(err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = targetEs.NewIndexOnly(ctx)
	if err != nil {
		panic(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	container := gabs.Wrap(mapping)
	container = container.Path(fmt.Sprintf("%s.mappings.%s", d.SourceIndex, d.SourceType))
	data := container.String()

	err = targetEs.PutMappingJson(ctx, data)
	if err != nil {
		panic(err)
	}
}

func (d *Dumper) getMinMaxTime() (minTime, maxTime *time.Time) {
	sourceEs := esutils.NewEs(d.SourceIndex, d.SourceType, esutils.WithClient(d.SourceClient))
	paging := &esutils.Paging{
		Skip:  0,
		Limit: 1,
		Sortby: []esutils.Sort{
			{
				Field:     d.Conf.DateField,
				Ascending: true,
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	page, err := sourceEs.Page(ctx, paging)
	if err != nil {
		panic(err)
	}
	if len(page.Docs) > 0 {
		first := page.Docs[0].(map[string]interface{})
		if value, ok := first[d.Conf.DateField]; ok {
			t, err := dateparse.ParseIn(value.(string), d.Zone)
			if err != nil {
				panic(err)
			}
			minTime = &t
		}
	}

	paging = &esutils.Paging{
		Skip:  0,
		Limit: 1,
		Sortby: []esutils.Sort{
			{
				Field:     d.Conf.DateField,
				Ascending: false,
			},
		},
	}
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	page, err = sourceEs.Page(ctx, paging)
	if err != nil {
		panic(err)
	}
	if len(page.Docs) > 0 {
		first := page.Docs[0].(map[string]interface{})
		if value, ok := first[d.Conf.DateField]; ok {
			t, err := dateparse.ParseIn(value.(string), d.Zone)
			if err != nil {
				panic(err)
			}
			maxTime = &t
		}
	}

	return
}

func (d *Dumper) dumpData() {
	sourceEs := esutils.NewEs(d.SourceIndex, d.SourceType, esutils.WithClient(d.SourceClient))
	targetEs := esutils.NewEs(d.TargetIndex, d.TargetType, esutils.WithClient(d.TargetClient))
	start := d.StartTime
	end := d.EndTime

	if start == nil || end == nil {
		min, max := d.getMinMaxTime()
		if start == nil {
			start = min
		}
		if end == nil {
			fixed := max.Add(1 * time.Second)
			end = &fixed
		}
	}

	localStart := start.In(time.Local)
	start = &localStart

	localEnd := end.In(time.Local)
	end = &localEnd

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	paging := &esutils.Paging{
		StartDate: start.Format(constants.FORMAT),
		EndDate:   end.Format(constants.FORMAT),
		DateField: d.Conf.DateField,
		Zone:      time.Local.String(),
	}
	if len(d.Includes) > 0 {
		paging.Includes = d.Includes
	}
	if len(d.Excludes) > 0 {
		paging.Excludes = d.Excludes
	}
	total, err := sourceEs.Count(ctx, paging)
	if err != nil {
		panic(err)
	}

	bar := progressbar.NewOptions64(
		total,
		progressbar.OptionSetWriter(os.Stderr),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Printf("\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
		progressbar.OptionUseANSICodes(true),
		progressbar.OptionEnableColorCodes(true),
	)

	step := d.Conf.Step
	if !d.Conf.Descending {
		for start.Before(*end) {
			_end := start.Add(step)
			if _end.After(*end) {
				_end = *end
			}
			paging = &esutils.Paging{
				StartDate:  start.Format(constants.FORMAT),
				EndDate:    _end.Format(constants.FORMAT),
				DateField:  d.Conf.DateField,
				Limit:      -1,
				ScrollSize: d.Conf.ScrollSize,
				Zone:       time.Local.String(),
			}
			if len(d.Includes) > 0 {
				paging.Includes = d.Includes
			}
			if len(d.Excludes) > 0 {
				paging.Excludes = d.Excludes
			}
			docs, err := sourceEs.List(context.Background(), paging, func(message json.RawMessage) (interface{}, error) {
				var p map[string]interface{}
				if err := json.Unmarshal(message, &p); err != nil {
					return nil, err
				}
				return p, nil
			})
			if err != nil {
				panic(err)
			}
			if len(docs) > 0 {
				err = targetEs.BulkSaveOrUpdate(context.Background(), docs)
				if err != nil {
					panic(err)
				}
			}
			start = &_end
			bar.Add(len(docs))
		}
	} else {
		for end.After(*start) {
			_start := end.Add(-step)
			if _start.Before(*start) {
				_start = *start
			}
			paging = &esutils.Paging{
				StartDate:  _start.Format(constants.FORMAT),
				EndDate:    end.Format(constants.FORMAT),
				DateField:  d.Conf.DateField,
				Limit:      -1,
				ScrollSize: d.Conf.ScrollSize,
				Zone:       time.Local.String(),
			}
			if len(d.Includes) > 0 {
				paging.Includes = d.Includes
			}
			if len(d.Excludes) > 0 {
				paging.Excludes = d.Excludes
			}
			docs, err := sourceEs.List(context.Background(), paging, func(message json.RawMessage) (interface{}, error) {
				var p map[string]interface{}
				if err := json.Unmarshal(message, &p); err != nil {
					return nil, err
				}
				return p, nil
			})
			if len(docs) > 0 {
				err = targetEs.BulkSaveOrUpdate(context.Background(), docs)
				if err != nil {
					panic(err)
				}
			}
			end = &_start
			bar.Add(len(docs))
		}
	}
}
