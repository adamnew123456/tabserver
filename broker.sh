#!/bin/sh
case "$1" in
    run)
        shift
        cd broker/broker
        exec dotnet run -c Release -- "$@" ;;

    benchmark)
        shift
        cd broker/brokerbench
        exec dotnet run -c Release -- "$@" ;;

    test)
        shift
        cd broker
        exec dotnet test "$@" ;;

    *)
        echo "Usage: build.sh (run ... | benchmark ... | test ...)" ;;
esac
