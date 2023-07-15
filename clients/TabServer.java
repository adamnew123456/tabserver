import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Scanner;

/// A tabserver client. Once started, it connects to a tabserver instance and
/// executes its commands on the provided database connection.
public class TabServer {
  /// An error indicating that the server sent unexpected data over the
  /// connection. This should never be handled, except to close the connection
  /// immediately.
  private static class ProtocolException extends Exception {
    public ProtocolException(String reason) {
      super(reason);
    }
  }

  private String _host;
  private int _port;
  private Connection _cnx;

  private Scanner _peerRecv;
  private BufferedWriter _peerSend;

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.out.println("TabServer DRIVER CONNECTION-STRING SERVER PORT");
      return;
    }

    String driverName = args[0];
    String connectionString = args[1];
    String server = args[2];
    int port = Integer.parseInt(args[3]);

    Class.forName(driverName);
    Connection cnx = DriverManager.getConnection(connectionString);
    try {
      new TabServer(server, port, cnx).start();
    } catch (ProtocolException err) {
      System.err.println("Aborting due to protocol error: " + err.getMessage());
      err.printStackTrace();
    }
  }

  public TabServer(String host, int port, Connection cnx) {
    _host = host;
    _port = port;
    _cnx = cnx;
  }

  /// Reads a line of base64-encoded data from the server and decodes it
  private String readData() throws IOException {
    String encodedBlob = readRaw();
    byte[] blob = Base64.getDecoder().decode(encodedBlob);
    return new String(blob, StandardCharsets.UTF_8);
  }

  /// Reads a line of data from the server without decoding it
  private String readRaw() throws IOException {
    _peerSend.flush();
    return _peerRecv.nextLine();
  }

  /// Encodes the data and sends it to the server
  private void sendData(String data) throws IOException {
    byte[] blob = data.getBytes(StandardCharsets.UTF_8);
    String encodedBlob = Base64.getEncoder().encodeToString(blob);
    sendRaw(encodedBlob);
  }

  /// Sends the data to the server without any encoding
  private void sendRaw(String data) throws IOException {
    _peerSend.write(data);
    _peerSend.write('\n');
  }

  /// Sends a list of columns and types to the server. Returns the number of
  /// columns that were sent.
  private int sendMetadata(ResultSetMetaData metadata) throws IOException, SQLException {
    // Not a separate spot in the protocol for reporting metadata errors. This
    // shouldn't normally fail, but if it does we want it to fail before we send
    // any part of METADATA.
    int columnCount = metadata.getColumnCount();
    String[] metadataLines = new String[2 * columnCount];
    for (int i = 0; i < columnCount; i++) {
      metadataLines[2*i] = metadata.getColumnLabel(i + 1);
      metadataLines[2*i + 1] = metadata.getColumnTypeName(i + 1);
    }

    sendRaw("METADATA");
    sendRaw(String.valueOf(columnCount));
    for (String line: metadataLines) {
      sendData(line);
    }

    return columnCount;
  }

  /// Sends a PAGE repsonse and gets the size of the next page. Returns 0 if the
  /// server sends an ABORT.
  private int getNextPageSize() throws IOException, ProtocolException {
    sendRaw("PAGE");
    String response = readRaw();
    if (response.equals("MORE")) {
      String rowCount = readRaw();
      int rowsLeft;
      try {
        rowsLeft = Integer.parseInt(rowCount);
      } catch (NumberFormatException err) {
        throw new ProtocolException("Non-numeric page size: " + rowCount);
      }

      if (rowsLeft <= 0) {
        throw new ProtocolException("Invalid page size: " + rowCount);
      }

      return rowsLeft;
    } else if (response.equals("ABORT")) {
      return 0;
    } else {
      throw new ProtocolException("Invalid response to PAGE command: " + response);
    }
  }

  /// Sends rows to the server, pausing to prompt for MORE/ABORT as needed.
  private void paginateQuery(ResultSet rs, int columnCount) throws IOException, ProtocolException, SQLException {
    String[] rowdataLines = new String[columnCount];
    int rowsLeft = 0;
    while (rs.next()) {
      if (rowsLeft == 0) {
        rowsLeft = getNextPageSize();
        if (rowsLeft == 0) break;
      }

      // Like METADATA, we have to buffer the row because we can't send an ERROR
      // repsonse after we've sent the ROW response. An error in any column has
      // to be reported up-front.
      rowsLeft--;
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

      sendRaw("ROW");
      for (String line: rowdataLines) {
        sendData(line);
      }
    }

    sendRaw("END");
  }

  /// Executes a query and sends back the affected rows, or the metadata and the
  /// row data.
  private void executeQuery() throws IOException, ProtocolException, SQLException {
    String query = readData();

    try (Statement stmt = _cnx.createStatement()) {
      stmt.execute(query);

      try (ResultSet rs = stmt.getResultSet()) {
        if (rs == null) {
          sendRaw("AFFECTED");
          sendRaw(String.valueOf(stmt.getUpdateCount()));
          return;
        }

        int columnCount = sendMetadata(rs.getMetaData());
        paginateQuery(rs, columnCount);
      }
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData(err.getMessage());
    }
  }

  /// Prepares a query and reports its metadata.
  private void prepareQuery() throws IOException {
    String query = readData();

    try (PreparedStatement stmt = _cnx.prepareStatement(query)) {
      ResultSetMetaData prepMetadata = stmt.getMetaData();
      if (prepMetadata == null) {
        sendRaw("AFFECTED");
        sendRaw("0");
      } else {
        sendMetadata(prepMetadata);
      }
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData(err.getMessage());
    }
  }

  /// Accepts a single connection from the server and executes its commands
  /// until it disconnects.
  public void start() throws IOException, ProtocolException, SQLException {
    try (Socket peer = new Socket(_host, _port)) {
      _peerRecv = new Scanner(new InputStreamReader(peer.getInputStream(), StandardCharsets.UTF_8));
      _peerSend = new BufferedWriter(new OutputStreamWriter(peer.getOutputStream(), StandardCharsets.UTF_8));

      DatabaseMetaData dbmd = _cnx.getMetaData();
      String identity = dbmd.getDriverName() + " " + dbmd.getDriverVersion();
      sendRaw("HELLO");
      sendData(identity);

      while (true) {
        String command;
        try {
          command = readRaw();

          if (command.equals("EXECUTE")) {
            executeQuery();
          } else if (command.equals("PREPARE")) {
            prepareQuery();
          } else {
            throw new ProtocolException("Invalid command: " + command);
          }
        } catch (NoSuchElementException err) {
          break;
        }
      }
    }
  }
}
