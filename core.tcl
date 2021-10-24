proc tabquery_connect {server port} {
    set conn [socket $server $port]
    fconfigure $conn -eofchar "\n"

    namespace eval ::tabserver::client_$conn {
        # Possible states:
        #
        # - IDLE: No command has been executed yet
        #
        # - {AWAIT_ACK next_state query}: The command has been sent, waiting for
        #   an OK to send the query.
        #
        # - AWAIT_EXECUTE: The query has been sent, waiting for a METADATA line, an ERROR line
        #   or an AFFECTED line
        #
        # - AWAIT_AFFECTED: The AFFECTED response has been received and we're
        #   waiting for the number of rows.
        #
        # - {AWAIT_METADATA prepared}: The METADATA response has been received and we're
        #   waiting for the number of lines.
        #
        # - {IN_METADATA prepared remaining data}: The metadata is in the process of being sent back, with
        #   $remaining total responses remaining
        #
        # - {AWAIT_ROW columns data}: The metadata is complete and we're waiting for either a
        #   ROW, END or ERROR line
        #
        # - {IN_ROW columns remaining data row}: The row is in the process of being
        #   sent back, with $remaining total responses remaining
        #
        # - AWAIT_PREPARE: The query has been sent, waiting for either a
        #   METADATA line or an ERROR line
        variable state IDLE

        # Will be a list in one of two forms:
        #
        # - {data {{row} {row} ...}}
        # - {error message}
        variable output {}

        proc send_raw {data} {
            variable conn
            puts $conn $data
            flush $conn
        }

        proc send_data {data} {
            send_raw [binary encode base64 [encoding convertto utf-8 $data]]
        }

        proc recv_raw {} {
            variable conn
            gets $conn data
            return $data
        }

        proc recv_data {} {
            return [encoding convertfrom utf-8 [binary decode base64 [recv_raw]]]
        }

        proc check_output {} {
            variable output
            variable conn
            vwait [namespace current]::output
            switch [lindex $output 0] {
                data {return [lindex $output 1]}
                error {error [lindex $output 1]}
            }
        }

        proc execute {query} {
            variable state
            variable output

            set state [list AWAIT_ACK AWAIT_EXECUTE $query]
            send_raw EXECUTE
            return [check_output]
        }

        proc prepare {query} {
            variable state
            variable pending_query
            variable output

            set state [list AWAIT_ACK AWAIT_PREPARE $query]
            send_raw PREPARE
            return [check_output]
        }

        proc on_read {} {
            variable state
            variable output

            switch [lindex $state 0] {
                IDLE {error "Unexpected data received in IDLE state. This is likely a bug."}

                AWAIT_ACK {
                    set reply [recv_raw]
                    set next_state [lindex $state 1]
                    set query [lindex $state 2]
                    switch $reply {
                        OK {
                            send_data $query
                            set state $next_state
                        }

                        ERROR {
                            set state IDLE
                            set output [list error "Server did not accept command: [recv_data]"]
                        }

                        default {
                            set state IDLE
                            set output [list error "Protocol error: unexpected EXECUTE response '$reply'"]
                        }
                    }
                }

                AWAIT_EXECUTE {
                    set reply [recv_raw]
                    switch $reply {
                        METADATA {set state [list AWAIT_METADATA 0]}
                        AFFECTED {set state AWAIT_AFFECTED}

                        ERROR {
                            set state IDLE
                            set output [list error "Server could not execute query: [recv_data]"]
                        }

                        default {
                            set state IDLE
                            set output [list error "Protocol error: unexpected EXECUTE response '$reply'"]
                        }
                    }
                }

                AWAIT_PREPARE {
                    set reply [recv_raw]
                    switch $reply {
                        METADATA {set state [list AWAIT_METADATA 1]}

                        ERROR {
                            set state IDLE
                            set output [list error "Server could not prepare query: [recv_data]"]
                        }

                        default {
                            set state IDLE
                            set output [list error "Protocol error: unexpected PREPARE response '$reply'"]
                        }
                    }
                }

                AWAIT_AFFECTED {
                    set reply [recv_raw]
                    set state IDLE
                    set output [list data [list AFFECTED INT $reply]]
                }

                AWAIT_METADATA {
                    set prepared [lindex $state 1]
                    set columns [recv_raw]
                    set state [list IN_METADATA $prepared [expr {$columns * 2}] {}]
                }

                IN_METADATA {
                    set prepared [lindex $state 1]
                    set remaining [lindex $state 2]
                    set metadata [lindex $state 3]
                    lappend metadata [recv_data]

                    incr remaining -1
                    if {$remaining == 0} {
                        set columns [expr {[llength $metadata] / 2}]

                        set column_titles {}
                        set column_types {}
                        foreach {title type} $metadata {
                            lappend column_titles $title
                            lappend column_types $type
                        }

                        if {$prepared} {
                            set state IDLE
                            set output [list data [list $column_titles $column_types]]
                        } else {
                            set state [list AWAIT_ROW $columns [list $column_titles $column_types]]
                        }
                    } else {
                        set state [list IN_METADATA $prepared $remaining $metadata]
                    }
                }

                AWAIT_ROW {
                    set columns [lindex $state 1]
                    set data [lindex $state 2]

                    set reply [recv_raw]
                    switch $reply {
                        ROW {
                            set state [list IN_ROW $columns $columns $data {}]
                        }

                        END {
                            set state IDLE
                            set output [list data $data]
                        }

                        ERROR {
                            set state IDLE
                            set output [list error "Server could not read rows: [recv_data]"]
                        }

                        default {
                            set state IDLE
                            set output [list error "Protocol error: unexpected EXECUTE response '$reply'"]
                        }
                    }
                }

                IN_ROW {
                    set remaining [lindex $state 2]
                    set row [lindex $state 4]

                    lappend row [recv_data]
                    incr remaining -1

                    set columns [lindex $state 1]
                    set data [lindex $state 3]
                    if {$remaining == 0} {
                        lappend data $row
                        set state [list AWAIT_ROW $columns $data]
                    } else {
                        set state [list IN_ROW $columns $remaining $data $row]
                    }
                }
            }
        }
    }

    namespace eval ::tabserver::client_$conn variable conn $conn
    chan event $conn readable "namespace eval ::tabserver::client_$conn on_read"

    return $conn
}

proc tabquery_close {conn} {
    close $conn
    namespace delete ::tabserver::client_$conn
}

proc tabquery_prepare {conn query} {
    return [namespace eval ::tabserver::client_$conn "prepare [list $query]"]
}

proc tabquery_execute {conn query} {
    return [namespace eval ::tabserver::client_$conn "execute [list $query]"]
}
