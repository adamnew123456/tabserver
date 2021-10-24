package require Tk

source core.tcl

proc connect {server port} {
    global CONN
    set CONN [tabquery_connect $server $port]
}

set OUTPUT_COUNTER 0

proc build_ui {} {
    global OUTPUT_COUNTER

    ttk::frame .h
    tk::canvas .h.history -xscrollcommand {.h.history_scrlx set} -yscrollcommand {.h.history_scrly set}
    tk::frame .h.history.frame
    tk::entry .sql
    tk::scrollbar .h.history_scrlx -orient horizontal -command {.h.history xview}
    tk::scrollbar .h.history_scrly -orient vertical -command {.h.history yview}

    tk::button .execute -text Execute -command {
        global CONN
        global OUTPUT_COUNTER
        set sql [.sql get]

        set was_error [catch {tabquery_execute $CONN $sql} result]

        set result_label ".h.history.frame.$OUTPUT_COUNTER"
        tk::label $result_label -font TkFixedFont -text $sql -relief groove -borderwidth 2
        pack $result_label

        incr OUTPUT_COUNTER

        if {$was_error} {
            set error_label ".h.history.frame.$OUTPUT_COUNTER"
            tk::label $error_label -font TkHeadingFont -text $result -relief groove -borderwidth 2
            pack $error_label
        } else {
            set result_grid ".h.history.frame.$OUTPUT_COUNTER"

            set rownum 0
            tk::frame $result_grid -relief groove -borderwidth 2
            foreach row $result {
                set colnum 0
                foreach col $row {
                    set cell "$result_grid.${rownum}_${colnum}"
                    if {$rownum < 2} {
                        set font TkHeadingFont
                    } else {
                        set font TkFixedFont
                    }

                    ttk::label $cell -font $font -text "| $col" -anchor w

                    grid $cell -row $rownum -column $colnum -sticky w
                    incr colnum
                }
                incr rownum
            }


            pack $result_grid
        }

        incr OUTPUT_COUNTER
        .h.history yview moveto 1
    }

    tk::button .prepare -text Prepare -command {
        global CONN
        global OUTPUT_COUNTER
        set sql [.sql get]

        set was_error [catch {tabquery_prepare $CONN $sql} result]

        set result_label ".h.history.frame.$OUTPUT_COUNTER"
        tk::label $result_label -font TkFixedFont -text $sql -relief groove -borderwidth 2
        pack $result_label

        incr OUTPUT_COUNTER

        if {$was_error} {
            set error_label ".h.history.frame.$OUTPUT_COUNTER"
            tk::label $error_label -font TkFixedFont -text $result -relief groove -borderwidth 2
            pack $error_label
        } else {
            set result_grid ".h.history.frame.$OUTPUT_COUNTER"

            set rownum 0
            tk::frame $result_grid -relief groove -borderwidth 2
            foreach row $result {
                set colnum 0
                foreach col $row {
                    set cell "$result_grid.${rownum}_${colnum}"
                    ttk::label $cell -font TkHeadingFont -text "| $col" -anchor w

                    grid $cell -row $rownum -column $colnum -sticky w
                    incr colnum
                }
                incr rownum
            }


            pack $result_grid
        }

        incr OUTPUT_COUNTER
        .h.history yview moveto 1
    }

    tk::button .clear -text Clear -command {
        global OUTPUT_COUNTER

        while {$OUTPUT_COUNTER > 0} {
            incr OUTPUT_COUNTER -1
            destroy .h.history.frame.$OUTPUT_COUNTER
        }
    }

    bind .h.history.frame "<Configure>" {
        .h.history configure -scrollregion [list %x %y %w %h]
    }

    .h.history create window 0 0 -anchor nw -window .h.history.frame

    pack .h.history_scrly -side right -fill y
    pack .h.history -fill both -expand 1
    pack .h.history_scrlx -side bottom -fill x
    pack .h -fill both -expand 1
    pack .clear -side bottom -fill x
    pack .prepare -side bottom -fill x
    pack .execute -side bottom -fill x
    pack .sql -side bottom -fill x
}

if {$argc == 0 || $argc > 2} {
    puts "$argv0 SERVER \[PORT\]"
    exit 1
}

if {$argc == 2} {
    connect [lindex $argv 0] [lindex $argv 1]
} else {
    set server [lindex $argv 0]
    connect [lindex $argv 0] 3306
}

build_ui
