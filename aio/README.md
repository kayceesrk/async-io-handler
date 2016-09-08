Asynchronous IO with event handlers. Uses Lwt_engine for the IO loop.

## Setup

```
$ opam remote add multicore git://github.com/ocamllabs/multicore-opam
$ opam switch 4.02.2+multicore
(* Install libev *)
$ opam install conf-libev lwt.2.5.1
$ make
```

## Run

Start the echo server by passing the IO engine name (`select` or `libev`) as
command-line argument. 

```
$ ./echo.native
Usage: ./echo.native [select|libev]
```
