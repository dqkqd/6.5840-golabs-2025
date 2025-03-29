mr-clean:
    rm -f **/**/*.so
    rm -f **/**/mr-tmpfile*
    rm -f **/**/mr-out*

[working-directory('src/main')]
@mr: mr-clean
    sh test-mr-many.sh 3

[working-directory('src/kvsrv1')]
@kvsrv-reliable:
    go test -v -run Reliable

[working-directory('src/kvsrv1')]
@kvsrv-all:
    go test -v

[working-directory('src/raft1')]
@lab-3a:
    go test -v -run 3A --race

@test-all: mr kvsrv-all lab-3a
