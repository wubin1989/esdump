# esdump
<p>
  <a href="https://godoc.org/github.com/wubin1989/esdump"><img src="https://godoc.org/github.com/wubin1989/esdump?status.png" alt="GoDoc"></a>
  <a href="https://github.com/wubin1989/esdump/actions/workflows/go.yml"><img src="https://github.com/wubin1989/esdump/actions/workflows/go.yml/badge.svg?branch=main" alt="Go"></a>
  <a href="https://codecov.io/gh/wubin1989/esdump/branch/main"><img src="https://codecov.io/gh/wubin1989/esdump/branch/main/graph/badge.svg?token=QRLPRAX885" alt="codecov"></a>
  <a href="https://goreportcard.com/report/github.com/wubin1989/esdump"><img src="https://goreportcard.com/badge/github.com/wubin1989/esdump" alt="Go Report Card"></a>
  <a href="https://github.com/wubin1989/esdump"><img src="https://img.shields.io/github/v/release/wubin1989/esdump?style=flat-square" alt="Release"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/License-MIT-yellow.svg" alt="License: MIT"></a>
</p>
<br/>

[![asciicast](https://asciinema.org/a/qudEF0BjfdFMHrvWOgEWblTGV.svg)](https://asciinema.org/a/qudEF0BjfdFMHrvWOgEWblTGV)

## Introduction

`esdump` is a migration CLI written in Go for migrating index mapping and data from one elasticsearch to another.

## Compatibility

|Elasticsearch version | Esdump version | Remarks              |
|----------------------|-----------------|----------------------|
|7.x                   | 2.x             | Coming soon.         |
|6.x                   | 1.x             | Actively maintained. |
|5.x                   | 1.x             | Actively maintained. |

## Install

- If go version < 1.17,
```shell
go get -v github.com/wubin1989/esdump@v1.0.0
```

- If go version >= 1.17,
```shell
go install -v github.com/wubin1989/esdump@v1.0.0
```

## Usage

```shell
➜  ~ esdump -h
migrate index from one elasticsearch to another

Usage:
  esdump [flags]

Flags:
  -d, --date string       date field of docs
      --desc              ascending or descending order by the date type field specified by date flag
  -e, --end string        end date, use time.Local as time zone, you may need to set TZ environment variable ahead
      --excludes string   excludes fields, multiple fields are separated by comma
  -h, --help              help for esdump
      --includes string   includes fields, multiple fields are separated by comma
  -i, --input string      source elasticsearch connection url
  -l, --limit int         limit for one scroll, it takes effect on the dumping speed (default 1000)
  -o, --output string     target elasticsearch connection url
  -s, --start string      start date, use time.Local as time zone, you may need to set TZ environment variable ahead
      --step duration     step duration (default 24h0m0s)
  -t, --type string       migration type, such as "mapping", "data", empty means both
  -v, --version           version for esdump
  -z, --zone string       time zone of the date type field specified by date flag (default "UTC")
```

## Example 

```shell
export TZ=Asia/Shanghai && esdump --input=http://localhost:9200/test --output=http://localhost:9200/test_dump --date=pubAt --start=2019-01-01 --zone=UTC --step=72h --excludes=html
```

## License

MIT
