require("dotenv").config();
const amqp = require("amqplib");
const { Client } = require("pg");

const {
  DB_HOST,
  DB_NAME,
  DB_USER,
  DB_PASSWORD,
  DB_PORT,
  BROKER_IP,
  BROKER_PORT,
  BROKER_QUEUE,
} = process.env;

const preCreateQuery = `CREATE TABLE if NOT EXISTS image_folders(
  id serial PRIMARY KEY,
  folder VARCHAR,
  origin VARCHAR,
  create_at TIMESTAMP,
  origin_folder VARCHAR UNIQUE
  ) ;
  
  CREATE TABLE if NOT EXISTS images(
  id serial PRIMARY KEY,
  filename VARCHAR,
  import_at TIMESTAMP,
  url VARCHAR(200) UNIQUE,
  folder_id INTEGER REFERENCES image_folders(id) ON DELETE CASCADE
  );`;

const handleTablePreCreate = async () => {
  const client = new Client({
    user: DB_USER,
    host: DB_HOST,
    database: DB_NAME,
    password: DB_PASSWORD,
    port: DB_PORT,
  });
  await client.connect();
  await client.query(preCreateQuery);
  await client.end();
};

//event function
async function handleAddDir(client, result, ackMsg) {
  const { folderPath, origin } = result;
  try {
    await client.connect();
    const queryText = `INSERT INTO image_folders(
        folder, 
        origin, 
        create_at , 
        origin_folder) 
        VALUES($1,$2,$3,$4 ) RETURNING id`;
    await client.query(queryText, [
      folderPath,
      origin,
      new Date(),
      `${origin}/${folderPath}`,
    ]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err.message);
    console.log(result);
    await client.end();
    ackMsg();
  }
}
async function handleUnlinkDir(client, result, ackMsg) {
  const { folderPath, origin } = result;
  const originFolder = `${origin}/${folderPath}`;
  try {
    await client.connect();
    const queryText = "DELETE FROM image_folders WHERE origin_folder = $1";
    await client.query(queryText, [originFolder]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err.message);
    console.log(result);
    ackMsg();
  }
}

async function handleAddImage(client, result, ackMsg) {
  const { folderPath, origin, filename } = result;
  const originFolder = `${origin}/${folderPath}`;
  const url = `${originFolder}/${filename}`;
  try {
    await client.connect();
    //check if folder exist
    const checkFolderQuery =
      "SELECT id FROM image_folders WHERE origin_folder = $1";
    let id;
    const checkFolderRes = await client.query(checkFolderQuery, [originFolder]);
    //folder not exist

    if (checkFolderRes.rows.length <= 0) {
      //add folder
      const addFolderQueryText = `INSERT INTO image_folders(
        folder, 
        origin, 
        create_at , 
        origin_folder) 
        VALUES($1,$2,$3,$4 ) RETURNING id`;
      const res = await client.query(addFolderQueryText, [
        folderPath,
        origin,
        new Date(),
        originFolder,
      ]);
      id = res.rows[0]["id"];
    } else {
      id = checkFolderRes.rows[0]["id"];
    }
    const insertText = `INSERT INTO images (
      filename,
      import_at,
      url,
      folder_id) VALUES($1,$2,$3,$4)`;
    await client.query(insertText, [filename, new Date(), url, id]);
    await client.end();
    ackMsg();
  } catch (err) {
    console.log(err.message);
    console.log(result);
    await client.end();
    ackMsg();
  }
}
async function handleUnlinkImage(client, result, ackMsg) {
  const { folderPath, origin, filename } = result;
  const originFolder = `${origin}/${folderPath}`;
  const url = `${originFolder}/${filename}`;
  try {
    await client.connect();
    const queryText = "DELETE FROM images WHERE url = $1";
    await client.query(queryText, [url]);
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
    const connection = await amqp.connect(`amqp://${BROKER_IP}:${BROKER_PORT}`);
    var ch = await connection.createChannel();
    // if (err != null) bail(err);
    ch.prefetch(2);
    await ch.assertQueue(BROKER_QUEUE);
    await handleTablePreCreate();
    ch.consume(BROKER_QUEUE, function (msg) {
      if (msg !== null) {
        const result = JSON.parse(msg.content.toString());
        const ackMsg = () => {
          return ch.ack(msg);
        };
        const client = new Client({
          user: DB_USER,
          host: DB_HOST,
          database: DB_NAME,
          password: DB_PASSWORD,
          port: DB_PORT,
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
