clean:
    rm -f **/**/*.so
    rm -f **/**/mr-tmpfile*
    rm -f **/**/mr-out*

[working-directory: 'src/main']
@mr-coordinator: clean
    go build -buildmode=plugin ../mrapps/wc.go
    go run mrcoordinator.go pg-*.txt

[working-directory: 'src/main']
@mr-client:
    go run mrworker.go wc.so
