package require Tk

source core.tcl

array set query_metadata {}
array set query_rows {}

proc max {a b} {
    if {$a > $b} {return $a} else {return $b}
}

proc query_callback {event id {value ""}} {
    global query_metadata query_rows

    switch $event {
        new_client {
            ttk::frame .tabs.$id
            .tabs add .tabs.$id -text "$value ($id)"

            tk::text .tabs.$id.log -state disabled
            pack .tabs.$id.log -fill both -expand 1

            tk::text .tabs.$id.query -height 6
            pack .tabs.$id.query -fill x

            ttk::frame .tabs.$id.actions
            tk::button .tabs.$id.actions.execute -text Execute -command "send_query $id tabquery_execute"
            tk::button .tabs.$id.actions.prepare -text Prepare -command "send_query $id tabquery_prepare"
            tk::button .tabs.$id.actions.clear -text Clear -command "clear_log $id"
            tk::button .tabs.$id.actions.kill -text Kill -command "tabquery_kill_connection $id 1"

            pack .tabs.$id.actions.execute -side left -fill both -expand 1
            pack .tabs.$id.actions.prepare -side left -fill both -expand 1
            pack .tabs.$id.actions.clear -side left -fill both -expand 1
            pack .tabs.$id.actions.kill -side left -fill both -expand 1
            pack .tabs.$id.actions -fill x

            ttk::frame .tabs.$id.cursor
            tk::button .tabs.$id.cursor.next -text Next -command "tabquery_resolve_page $id 1"
            tk::button .tabs.$id.cursor.abort -text Abort -command "tabquery_resolve_page $id 0"

            pack .tabs.$id.cursor.next -side left -fill both -expand 1
            pack .tabs.$id.cursor.abort -side left -fill both -expand 1
        }

        drop_client {
            pack forget .tabs.$id.query
            pack forget .tabs.$id.actions
            pack forget .tabs.$id.cursor
            write_alert $id "Connection closed"
        }

        metadata {
            set query_metadata($id) $value
        }

        data {
            lappend query_rows($id) $value
        }

        page {
            render_table $id

            set query_rows($id) {}
            .tabs.$id.log configure -state normal
            .tabs.$id.log insert end "------ NEXT PAGE ------\n"
            .tabs.$id.log configure -state disabled

            pack .tabs.$id.cursor -fill x
        }

        page_confirm {
            pack forget .tabs.$id.cursor
        }

        done {
            render_table $id
            .tabs.$id.query configure -state normal
        }

        error {
            write_alert $id $value
            .tabs.$id.query configure -state normal
        }
    }
}

proc clear_log {id} {
    .tabs.$id.log configure -state normal
    .tabs.$id.log delete 1.0 end
    .tabs.$id.log configure -state disabled
}

proc write_alert {id message} {
    .tabs.$id.log configure -state normal
    .tabs.$id.log insert end "\n\nAlert: $message\n\n"
    .tabs.$id.log configure -state disabled
}

proc send_query {id command} {
    global query_metadata query_rows
    set query_metadata($id) {}
    set query_rows($id) {}

    set sql [string trim [.tabs.$id.query get 1.0 end]]

    .tabs.$id.log configure -state normal
    .tabs.$id.log insert end "\n\nSQL:\n$sql\n\n"
    .tabs.$id.log configure -state disabled

    $command $id $sql
    .tabs.$id.query configure -state disabled
}

proc render_table {id} {
    global query_metadata query_rows

    set rows {}
    set maxlength {}

    set row {}
    foreach field $query_metadata($id) {
        set column [lindex $field 0]
        lappend row $column
        lappend maxlength [string length $column]
    }
    lappend rows $row

    set i 0
    set row {}
    foreach field $query_metadata($id) {
        set type [lindex $field 1]
        lappend row $type
        lset maxlength $i [max [lindex $maxlength $i] [string length $type]]
        incr i
    }
    lappend rows $row

    foreach row $query_rows($id) {
        lappend rows $row

        set i 0
        foreach cell $row {
            lset maxlength $i [max [lindex $maxlength $i] [string length $cell]]
            incr i
        }
    }

    .tabs.$id.log configure -state normal

    foreach row $rows {
        set i 0
        foreach cell $row {
            set justify [lindex $maxlength $i]
            .tabs.$id.log insert end [format "| %${justify}s " $cell]
            incr i
        }

        .tabs.$id.log insert end "|\n"
    }

    .tabs.$id.log configure -state disabled
}

proc build_root_ui {} {
    ttk::notebook .tabs
    pack .tabs -fill both -expand 1
}

if {$argc != 1} {
    puts "$argv0 PORT"
    exit 1
}

tabquery_bind [lindex $argv 0] query_callback
build_root_ui
