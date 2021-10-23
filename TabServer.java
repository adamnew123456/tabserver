import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class TabServer {
  private int _port;
  private Connection _cnx;

  private Scanner _peerRecv;
  private BufferedWriter _peerSend;

  public TabServer(int port, Connection cnx) {
    _port = port;
    _cnx = cnx;
  }

  private String readData() throws IOException {
    String encodedBlob = readRaw();
    byte[] blob = Base64.getDecoder().decode(encodedBlob);
    return new String(blob, StandardCharsets.UTF_8);
  }

  private String readRaw() throws IOException {
    _peerSend.flush();
    return _peerRecv.nextLine();
  }

  private void sendData(String data) throws IOException {
    byte[] blob = data.getBytes(StandardCharsets.UTF_8);
    String encodedBlob = Base64.getEncoder().encodeToString(blob);
    sendRaw(encodedBlob);
  }

  private void sendRaw(String data) throws IOException {
    _peerSend.write(data);
    _peerSend.write('\n');
  }

  private void executeQuery() throws IOException {
    String query = readData();

    Statement stmt = null;
    try {
      stmt = _cnx.createStatement();
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData("Could not execute: " + err.getMessage());
      return;
    }

    ResultSet rs = null;
    int affected = 0;
    try {
      stmt.execute(query);
      rs = stmt.getResultSet();
      affected = stmt.getUpdateCount();
    } catch (SQLException err) {
      if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
      if (rs != null) try { rs.close(); } catch (SQLException ignore) { }

      sendRaw("ERROR");
      sendData("Could not execute: " + err.getMessage());
      return;
    }

    if (rs != null) {
      String[] metadataLines = null;
      int columnCount = 0;

      try {
        // Metadata errors can only be reported up-front, so build the response first
        ResultSetMetaData metadata = rs.getMetaData();
        columnCount = metadata.getColumnCount();
        metadataLines = new String[2 * columnCount];
        for (int i = 0; i < columnCount; i++) {
          metadataLines[2*i] = metadata.getColumnLabel(i + 1);
          metadataLines[2*i + 1] = metadata.getColumnTypeName(i + 1);
        }
      } catch (SQLException err) {
        if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
        if (rs != null) try { rs.close(); } catch (SQLException ignore) { }

        sendRaw("ERROR");
        sendData("Could not build metadata: " + err.getMessage());
        return;
      }

      sendRaw("METADATA");
      sendRaw("" + columnCount);
      for (String line: metadataLines) {
        sendData(line);
      }

      String[] rowdataLines = new String[columnCount];

      try {
        // Same as before, row errors can only be emitted on a per-row basis
        while (rs.next()) {
          try {
            for (int i = 0; i < columnCount; i++) {
              Object value = rs.getObject(i + 1);

              if (value == null) {
                rowdataLines[i] = "<null>";
              } else if (value instanceof byte[]) {
                rowdataLines[i] = Base64.getEncoder().encodeToString((byte[]) value);
              } else {
                rowdataLines[i] = value.toString();
              }
            }
          } catch (SQLException err) {
            if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
            if (rs != null) try { rs.close(); } catch (SQLException ignore) { }

            sendRaw("ERROR");
            sendData("Could not build row data: " + err.getMessage());
            return;
          }

          sendRaw("ROW");
          for (String line: rowdataLines) {
            sendData(line);
          }
        }
      } catch (SQLException err) {
        if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
        if (rs != null) try { rs.close(); } catch (SQLException ignore) { }

        sendRaw("ERROR");
        sendData("Could not build row data: " + err.getMessage());
        return;
      }

      sendRaw("END");
    } else {
      sendRaw("AFFECTED");
      sendRaw("" + affected);
    }
  }

  private void prepareQuery() throws IOException {
    String query = readData();

    PreparedStatement stmt = null;
    try {
      stmt = _cnx.prepareStatement(query);
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData("Could not prepare: " + err.getMessage());
      return;
    }

    String[] metadataLines = null;
    int columnCount = 0;

    try {
      // Metadata errors can only be reported up-front, so build the response first
      ResultSetMetaData metadata = stmt.getMetaData();
      columnCount = metadata.getColumnCount();
      metadataLines = new String[2 * columnCount];
      for (int i = 0; i < columnCount; i++) {
        metadataLines[2*i] = metadata.getColumnLabel(i + 1);
        metadataLines[2*i + 1] = metadata.getColumnTypeName(i + 1);
      }
    } catch (SQLException err) {
      if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }

      sendRaw("ERROR");
      sendData("Could not build metadata: " + err.getMessage());
      return;
    }

    sendRaw("METADATA");
    sendRaw("" + columnCount);
    for (String line: metadataLines) {
      sendData(line);
    }
  }

  public void start() throws IOException {
    ServerSocket server = null;
    Socket peer = null;
    try {
      server = new ServerSocket(_port, 1);
      peer = server.accept();

      _peerRecv = new Scanner(new InputStreamReader(peer.getInputStream(), StandardCharsets.US_ASCII));
      _peerSend = new BufferedWriter(new OutputStreamWriter(peer.getOutputStream(), StandardCharsets.US_ASCII));

      while (true) {
        String command;
        try {
          command = readRaw();
        } catch (NoSuchElementException err) {
          break;
        }

        if (command.equals("EXECUTE")) {
          sendRaw("OK");
          executeQuery();
        } else if (command.equals("PREPARE")) {
          sendRaw("OK");
          prepareQuery();
        } else {
          sendRaw("ERROR");
          sendData("Illegal command");
        }
      }
    } finally {
      if (peer != null) peer.close();
      if (server != null) server.close();
    }
  }
}
