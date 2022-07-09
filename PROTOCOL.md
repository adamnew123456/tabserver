# tabserver Protocol

- Line-based for ease of use with Tcl (line here meaning linefeed)
- Supports prepared statement to report just the schema that a query would report
- Supports pagination so large responses don't have to be fully sent
- Supports a max return count so the viewer doesn't have to render everything
- Non-command text is assumed to be encoded as UTF-8 and base64 encoded

## Lifecycle

1. The user interacts with a query processor, which accepts SQL commands in
   addition to connection management commands. The query processor acts as a
   server.
   
2. Each database connection acts as a client. It connects to the processor and
   identifies itself.
   
3. The user types in SQL queries and those are sent to the currently active
   client.
   
4. The user can also type in commands to change the current client or forcibly
   disconnect a client.
   
## Introduction Command

When connecting to the server, each database client must introduce itself so the
user knows which connection corresponds to which database. This is accomplished
with a `HELLO` command. This is followed by a description of the connection that
is shown to the user when they list the available clients.

```
client> HELLO
client> PostgreSQL on pgsql.lan
```

The server does not accept data from a client out of turn. If the first command
on the connection is not `HELLO`, or the client sends something to the server
that was not previously requested, the server will immediately terminate the
connection.

## Server Commands

- `EXECUTE` The line after the `EXECUTE` must be a SQL query. The client executes
  the query and returns:
  - An `ERROR` response
  - A `METADATA` response, then one or more `ROW` responses and finally an `END`
    response or an `ERROR` response. 
    - Every 100 rows, the client will  send a `MORE` response to see if the
      server is still interested in more rows.  The server replies with either a
      `MORE` response or an `ABORT` response. For a `MORE`, the client will
      continue sending `ROW` responses after the `OK`.  But if it receives an
      `ABORT` the client will send no more replies for this query.
  - An `AFFECTED` response
    
- `PREPARE` The line after the `PREPARE` must be a SQL query. The client prepares
  the query and fetches its metadata, but does not execute it. This returns:
  - An `ERROR` response
  - A `METADATA` response

- Any other command will trigger an `ERROR` response
  
## Server Responses

- `ERROR` The line after the `ERROR` is a description of the error message

- `METADATA` The line after the `METADATA` is a base 10 number that gives the
  number of columns (it is not base64 encoded). Then two lines per column
  follows giving the column's label and type.
  
- `ROW` always follows an existing `METADATA` command, so the number of columns
  to expect is known in advance. One line per column follows the `ROW` command
  and contains the data rendered as a string.
  
- `PAGE` indicates that more rows may follow, but the client has to request them.
  This allows for aborting queries that produce more rows than anticipated.
  
- `END` indicates that no more rows follow.

- `AFFECTED` The line after the `AFFECTED` is a base 10 number that gives the
  number of rows affected by the previous command. Should only be returned for
  non-SELECT queries.

## Example session

Here strings are not base64 encoded for ease of reading:

```
client> HELLO
client> PostgreSQL on db.lan
server> EXECUTE
server> SELECT COUNT(*) FROM t
client> METADATA
client> 1
client> COUNT
client> INT
client> ROW
client> 10
client> END
server> PREPARE
server> SELECT * FROM t
client> METADATA
client> 3
client> id
client> INT
client> name
client> VARCHAR
client> balance
client> DECIMAL
server> INSERT INTO t(balance) VALUES (12.34)
client> AFFECTED
client> 1
```
