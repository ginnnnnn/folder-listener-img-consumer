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

const preCreateQuery = `CREATE TABLE if NOT EXISTS source_folder(
  id serial PRIMARY KEY,
  folder VARCHAR,
  origin VARCHAR,
  create_at TIMESTAMP,
  origin_folder VARCHAR UNIQUE
  ) ;
  
  CREATE TABLE if NOT EXISTS source_image(
  id serial PRIMARY KEY,
  filename VARCHAR,
  import_at TIMESTAMP,
  url VARCHAR(200) UNIQUE,
  "sourceFolderId" INTEGER REFERENCES source_folder(id) ON DELETE CASCADE
  );
  `;
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
    const queryText = `INSERT INTO source_folder(
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
    const queryText = "DELETE FROM source_folder WHERE origin_folder = $1";
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
      "SELECT id FROM source_folder WHERE origin_folder = $1";
    let id;
    const checkFolderRes = await client.query(checkFolderQuery, [originFolder]);
    //folder not exist

    if (checkFolderRes.rows.length <= 0) {
      //add folder
      const addFolderQueryText = `INSERT INTO source_folder(
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
    const insertText = `INSERT INTO source_image (
      filename,
      import_at,
      url,
      "sourceFolderId") VALUES($1,$2,$3,$4)`;
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
    const queryText = "DELETE FROM source_image WHERE url = $1";
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
    ch.prefetch(3);
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
