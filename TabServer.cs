using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

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

    private string ReadRaw()
    {
        _peerSend.Flush();
        return _peerRecv.ReadLine();
    }

    private string ReadData()
    {
        var encodedBlob = ReadRaw();
        var blob = Convert.FromBase64String(encodedBlob);
        return Encoding.UTF8.GetString(blob);
    }

    private void WriteRaw(string line)
    {
        _peerSend.Write(line);
        _peerSend.Write('\n');
    }

    private void WriteData(string line)
    {
        var blob = Encoding.UTF8.GetBytes(line);
        var encodedBlob = Convert.ToBase64String(blob);
        WriteRaw(encodedBlob);
    }

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
                        WriteRaw("AFFECTED");
                        WriteRaw("" + affected);
                        return;
                    }

                    // Buffer all the field metadata since we can't report an error
                    // once the METADATA reply has been sent
                    var columnCount = reader.FieldCount;
                    var metadataLines = new string[columnCount * 2];
                    for (var i = 0; i < columnCount; i++)
                    {
                        metadataLines[2 * i] = reader.GetName(i);
                        metadataLines[2 * i + 1] = reader.GetFieldType(i).Name;
                    }

                    WriteRaw("METADATA");
                    WriteRaw("" + columnCount);
                    foreach (var line in metadataLines)
                    {
                        WriteData(line);
                    }

                    // As above, we can only send an error after buffering the whole row
                    int pageLeft = 25;
                    var dataLines = new string[columnCount];
                    while (reader.Read())
                    {
                        if (pageLeft == 0)
                        {
                          WriteRaw("PAGE");
                          string response = ReadRaw();
                          if (response == "MORE")
                          {
                            pageLeft = 25;
                          }
                          else if (response == "ABORT")
                          {
                            break;
                          }
                        }

                        for (var i = 0; i < columnCount; i++)
                        {
                            if (reader.IsDBNull(i))
                            {
                                dataLines[i] = "<null>";
                            } else
                            {
                                var value = reader[i];
                                if (value is byte[])
                                {
                                    dataLines[i] = Convert.ToBase64String(value as byte[]);
                                }
                                else
                                {
                                    dataLines[i] = value.ToString();
                                }
                            }
                        }

                        WriteRaw("ROW");
                        foreach (var line in dataLines)
                        {
                            WriteData(line);
                        }
                    }

                    WriteRaw("END");
                }
            }
            catch (Exception err)
            {
                WriteRaw("ERROR");
                WriteData("Could not execute: " + err.Message);
                return;
            }
        }
    }

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
                    // Buffer all the field metadata since we can't report an error
                    // once the METADATA reply has been sent
                    var columnCount = reader.FieldCount;
                    var metadataLines = new string[columnCount * 2];
                    for (var i = 0; i < columnCount; i++)
                    {
                        metadataLines[2 * i] = reader.GetName(i);
                        metadataLines[2 * i + 1] = reader.GetFieldType(i).Name;
                    }

                    WriteRaw("METADATA");
                    WriteRaw("" + columnCount);
                    foreach (var line in metadataLines)
                    {
                        WriteData(line);
                    }
                }
            }
            catch (Exception err)
            {
                WriteRaw("ERROR");
                WriteData("Could not prepare: " + err.Message);
                return;
            }
        }
    }

    public void Start()
    {
        using (var peer = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.IP))
        {
            peer.Connect(_host, _port);
            var stream = new NetworkStream(peer, FileAccess.ReadWrite);
            _peerRecv = new StreamReader(stream);
            _peerSend = new StreamWriter(stream);

            WriteRaw("HELLO");
            WriteData(_cnx.DataSource == null ? "???" : _cnx.DataSource);
            _peerSend.Flush();

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
