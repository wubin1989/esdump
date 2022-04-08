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

```shell
➜  esdump git:(main) ✗ export TZ=Asia/Shanghai && esdump --input=http://localhost:9200/policy_test \
--output=http://localhost:9200/policy_esdump --date=pubAt --start=2019-01-01 --zone=UTC --step=72h --excludes=html
   6% |█████████                                                                 | (11226/167100, 152 it/s) [1m7s:17m3s]
```
