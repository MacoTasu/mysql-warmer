# mysql-warmer
[![Build Status](https://travis-ci.org/MacoTasu/mysql-warmer.svg?branch=master)](https://travis-ci.org/MacoTasu/mysql-warmer)

mysql warmer for myisam.

## Description
### Usage
```
mysql-warmer --host=<mysql host> --user=<db username> --port=<mysql port>
```
If you need password, use `--password=` or `-p=` options. Please enter the password at the stdin of the prompt.

### Install
- go get
 ```
 go get -u github.com/MacoTasu/mysql-warmer
 ```

## LICENSE
MIT License

Copyright (c) 2017 Makoto Shiga
