proc tabquery_connect {server port} {
    set conn [socket $server $port]
    fconfigure $conn -eofchar "\n"
    return $conn
}

proc tabquery_close {conn} {
    close $conn
}

proc tabquery_send_data {conn data} {
    set raw [encoding convertto utf-8 $data]
    puts $conn [binary encode base64 $raw]
}

proc tabquery_recv_data {conn} {
    gets $conn raw_binary
    set raw [binary decode base64 $raw_binary]
    return [encoding convertfrom utf-8 $raw]
}

proc tabquery_read_metadata {conn} {
    # Assumes the METADATA token is already read
    gets $conn cols
    set col_defs {}

    while {$cols > 0} {
        set label [tabquery_recv_data $conn]
        set type [tabquery_recv_data $conn]
        lappend col_defs $label $type

        incr cols -1
    }

    return $col_defs
}

proc tabquery_read_row {conn col_count} {
    # Assumes the ROW token is already read
    set data {}
    while {$col_count > 0} {
        lappend data [tabquery_recv_data $conn]
        incr col_count -1
    }

    return $data
}

proc tabquery_prepare {conn query} {
    puts $conn PREPARE
    flush $conn

    gets $conn reply
    switch $reply {
        OK {
        }

        ERROR {
            set message [tabquery_recv_data $conn]
            error "Server did not understand PREPARE: $message"
        }

        default {
            error "Protocol error: Unexpected EXECUTE reply '$reply'"
        }
    }

    tabquery_send_data $conn $query

    flush $conn
    gets $conn reply
    switch $reply {
        ERROR {
            set message [tabquery_recv_data $conn]
            error "Server error during execute: $message"
        }

        METADATA {
            return [tabquery_read_metadata $conn]
        }

        default {
            error "Protocol error: Unexpected PREPARE reply '$reply'"
        }
    }
}

proc tabquery_execute {conn query} {
    puts $conn EXECUTE

    flush $conn
    gets $conn reply
    switch $reply {
        OK {
        }

        ERROR {
            set message [tabquery_recv_data $conn]
            error "Server did not understand EXECUTE: $message"
        }

        default {
            error "Protocol error: Unexpected EXECUTE reply '$reply'"
        }
    }


    tabquery_send_data $conn $query

    set output {}

    flush $conn
    gets $conn reply
    switch $reply {
        ERROR {
            set message [tabquery_recv_data $conn]
            error "Server error: $message"
        }

        METADATA {
            set metadata [tabquery_read_metadata $conn]
            set output [list $metadata]
        }

        AFFECTED {
            gets $conn rowcount
            return [list [list AFFECTED INT] [list $rowcount]]
        }

        default {
            error "Protocol error: Unexpected EXECUTE reply '$reply'"
        }
    }

    set col_count [expr {[llength $metadata] / 2}]
    set done 0
    while {!$done} {
        gets $conn reply
        switch $reply {
            ROW {
                set row [tabquery_read_row $conn $col_count]
                lappend output $row
            }

            END {
                set done 1
            }

            ERROR {
                set message [tabquery_recv_data $conn]
                error "Server error during read: $message"
            }

            default {
                error "Protocol error: Unexpected EXECUTE data reply '$reply'"
            }
        }
    }

    return $output
}
