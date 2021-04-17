const amqp = require("amqplib");
const { Client } = require("pg");
var q = "import_images";

//event function
async function handleAddDir(client, result, ackMsg) {
  try {
    await client.connect();
    const checkQuery = "SELECT * FROM image_folders WHERE path = $1";
    const checkRes = await client.query(checkQuery, [result.folderPath]);
    if (checkRes.rows.length > 0) {
      throw new Error("Dir exist");
    }
    const queryText =
      "INSERT INTO image_folders( path, create_at) VALUES($1 , $2)";
    await client.query(queryText, [result.folderPath, new Date()]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err);
    console.log(result);
    await client.end();
    ackMsg();
  }
}
async function handleUnlinkDir(client, result, ackMsg) {
  try {
    await client.connect();
    const queryText = "DELETE FROM image_folders WHERE path = $1";
    await client.query(queryText, [result.folderPath]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err);
    console.log(result);
    ackMsg();
  }
}

async function handleAddImage(client, result, ackMsg) {
  try {
    await client.connect();
    const checkFolderQuery = "SELECT * FROM image_folders WHERE path = $1";
    const checkFolderRes = await client.query(checkFolderQuery, [
      result.folderPath,
    ]);
    //folder not exist
    if (checkFolderRes.rows.length <= 0) {
      const addFolderText =
        "INSERT INTO image_folders( path, create_at) VALUES($1 , $2)";
      await client.query(addFolderText, [result.folderPath, new Date()]);
    }
    const queryText =
      "SELECT * FROM images WHERE filename = $1 AND folder_path = $2";
    const checkRes = await client.query(queryText, [
      result.filename,
      result.folderPath,
    ]);
    if (checkRes.rows.length > 0) {
      //file exist
      throw new Error("file exist");
    }
    const insertText =
      "INSERT INTO images (filename,import_at,folder_path) VALUES($1,$2,$3)";
    const res = await client.query(insertText, [
      result.filename,
      new Date(),
      result.folderPath,
    ]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err);
    console.log(result);
    await client.end();
    ackMsg();
  }
}
async function handleUnlinkImage(client, result, ackMsg) {
  try {
    await client.connect();
    const queryText =
      "DELETE FROM images WHERE filename = $1 AND folder_path = $2";
    await client.query(queryText, [result.filename, result.folderPath]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err);
    console.log(result);
    await client.end();
    ackMsg();
  }
}

// consumer

async function connect() {
  try {
    const connection = await amqp.connect("amqp://localhost:5672");
    var ch = await connection.createChannel();
    // if (err != null) bail(err);
    ch.prefetch(2);
    await ch.assertQueue("import_images");
    ch.consume(q, function (msg) {
      if (msg !== null) {
        const result = JSON.parse(msg.content.toString());
        const ackMsg = () => {
          return ch.ack(msg);
        };
        const client = new Client({
          user: "postgres",
          host: "localhost",
          database: "aimodel",
          password: "postgres",
          port: 5432,
        });
        if (result.event === "addDir") {
          handleAddDir(client, result, ackMsg);
        }
        if (result.event === "unlinkDir") {
          handleUnlinkDir(client, result, ackMsg);
        }
        if (result.event === "add") {
          handleAddImage(client, result, ackMsg);
        }
        if (result.event === "unlink") {
          handleUnlinkImage(client, result, ackMsg);
        }
      }
    });
  } catch (err) {
    console.log(err);
  }
}
connect();

// client.connect("amqp://localhost", function (err, conn) {
//   if (err != null) bail(err);
// });
