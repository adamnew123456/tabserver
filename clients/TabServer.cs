using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

/// An error indicating that the server sent malformed data or commands. Should
/// not be handled outside of immediately closing the connection.
class ProtocolException : Exception
{
    public ProtocolException(string message): base(message) {}
}

/// A tabserver client. Once started, it connects to a tabserver instance and
/// executes its commands on the provided database connection.
public class TabServer
{
    private DbConnection _cnx;
    private string _host;
    private int _port;

    private StreamReader _peerRecv;
    private StreamWriter _peerSend;

    public TabServer(string host, int port, DbConnection cnx)
    {
        _host = host;
        _port = port;
        _cnx = cnx;
    }

    /// Reads a line of base64-encoded data from the server and decodes it
    private string ReadData()
    {
        var encodedBlob = ReadRaw();
        var blob = Convert.FromBase64String(encodedBlob);
        return Encoding.UTF8.GetString(blob);
    }

    /// Reads a line of data from the server without decoding it
    private string ReadRaw()
    {
        _peerSend.Flush();
        return _peerRecv.ReadLine();
    }

    /// Encodes the data and sends it to the server
    private void SendData(string line)
    {
        var blob = Encoding.UTF8.GetBytes(line);
        var encodedBlob = Convert.ToBase64String(blob);
        SendRaw(encodedBlob);
    }

    /// Sends the data to the server without any encoding
    private void SendRaw(string line)
    {
        _peerSend.Write(line);
        _peerSend.Write('\n');
    }

    /// Sends a list of columns and types to the server. Returns the number of
    /// columns that were sent.
    private int SendMetadata(IDataReader reader)
    {
        // Not a separate spot in the protocol for reporting metadata errors. This
        // shouldn't normally fail, but if it does we want it to fail before we send
        // any part of METADATA.
        var columnCount = reader.FieldCount;
        var metadataLines = new string[2 * columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            metadataLines[2*i] = reader.GetName(i);
            metadataLines[2*i + 1] = reader.GetFieldType(i).Name;
        }

        SendRaw("METADATA");
        SendRaw(columnCount.ToString());
        foreach (var line in metadataLines) {
            SendData(line);
        }

        return columnCount;
    }

    /// Sends a PAGE repsonse and gets the size of the next page. Returns 0 if the
    /// server sends an ABORT.
    private int GetNextPageSize()
    {
        SendRaw("PAGE");
        var response = ReadRaw();
        if (response == "MORE")
        {
            var rowCount = ReadRaw();
            int rowsLeft;
            try
            {
                rowsLeft = int.Parse(rowCount);
            }
            catch (FormatException err)
            {
                throw new ProtocolException($"Non-numeric page size: {rowCount}");
            }

            if (rowsLeft <= 0)
            {
                throw new ProtocolException($"Invalid page size: {rowCount}");
            }

            return rowsLeft;
        }
        else if (response == "ABORT")
        {
            return 0;
        }
        else
        {
            throw new ProtocolException("Invalid response to PAGE command");
        }
    }

    /// Sends rows to the server, pausing to prompt for MORE/ABORT as needed.
    private void PaginateQuery(IDataReader reader, int columnCount)
    {
        var rowdataLines = new String[columnCount];
        var rowsLeft = 0;
        while (reader.Read())
        {
            if (rowsLeft == 0)
            {
                rowsLeft = GetNextPageSize();
                if (rowsLeft == 0) break;
            }

            // Like METADATA, we have to buffer the row because we can't send an ERROR
            // repsonse after we've sent the ROW response. An error in any column has
            // to be reported up-front.
            rowsLeft--;
            for (int i = 0; i < columnCount; i++)
            {
                if (reader.IsDBNull(i))
                {
                    rowdataLines[i] = "<null>";
                }
                else
                {
                    var value = reader[i];
                    if (value is byte[])
                    {
                        rowdataLines[i] = Convert.ToBase64String(value as byte[]);
                    }
                    else
                    {
                        rowdataLines[i] = value.ToString();
                    }
                }
            }

            SendRaw("ROW");
            foreach (var line in rowdataLines)
            {
                SendData(line);
            }
        }

        SendRaw("END");
    }

    /// Executes a query and sends back the affected rows, or the metadata and the
    /// row data.
    private void ExecuteQuery()
    {
        var query = ReadData();
        using (var command = _cnx.CreateCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = query;

            try
            {
                using (var reader = command.ExecuteReader())
                {
                    if (reader.FieldCount == 0)
                    {
                        var affected = reader.RecordsAffected;
                        SendRaw("AFFECTED");
                        SendRaw(affected.ToString());
                        return;
                    }

                    var columnCount = SendMetadata(reader);
                    PaginateQuery(reader, columnCount);
                }
            }
            catch (Exception err)
            {
                SendRaw("ERROR");
                SendData("Could not execute: " + err.Message);
                return;
            }
        }
    }

    /// Prepares a query and reports its metadata.
    private void PrepareQuery()
    {
        var query = ReadData();
        using (var command = _cnx.CreateCommand())
        {
            command.CommandType = CommandType.Text;
            command.CommandText = query;

            try
            {
                using (var reader = command.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    if (reader.FieldCount == 0)
                    {
                        var affected = reader.RecordsAffected;
                        SendRaw("AFFECTED");
                        SendRaw(affected.ToString());
                    }
                    else
                    {
                        SendMetadata(reader);
                    }
                }
            }
            catch (Exception err)
            {
                SendRaw("ERROR");
                SendData("Could not prepare: " + err.Message);
                return;
            }
        }
    }

    /// Accepts a single connection from the server and executes its commands
    /// until it disconnects.
    public void Start()
    {
        using (var peer = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.IP))
        {
            peer.Connect(_host, _port);
            var stream = new NetworkStream(peer, FileAccess.ReadWrite);
            _peerRecv = new StreamReader(stream);
            _peerSend = new StreamWriter(stream);

            SendRaw("HELLO");
            SendData(_cnx.DataSource == null ? "???" : _cnx.DataSource);

            var done = false;
            while (!done)
            {
                var command = ReadRaw();
                if (command == null) return;

                switch (command)
                {
                    case "EXECUTE":
                        ExecuteQuery();
                        break;
                    case "PREPARE":
                        PrepareQuery();
                        break;
                    default:
                        break;
                }
            }
        }
    }
}
