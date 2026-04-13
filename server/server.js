const express = require("express");
const cors = require("cors");
const dotenv = require("dotenv");
const pool = require("./db");
const router = require("./router/routes");
dotenv.config();

const app = express();

const allowedOrigins = [
  "http://localhost:3000",
  "https://your-netlify-app.netlify.app", // Will update after Netlify deploy
  process.env.FRONTEND_URL,

];

app.use(
  cors({
    origin: function (origin, callback) {
      if (!origin) return callback(null, true);
      if (allowedOrigins.indexOf(origin) === -1) {
        const msg =
          "The CORS policy for this site does not allow access from the specified Origin.";
        return callback(new Error(msg), false);
      }
      return callback(null, true);
    },
    credentials: true,
  }),
);
app.use(express.json());
app.use("/schmgt", router);
app.use("/uploads", express.static("uploads"));
app.use("/uploads/school-logo", express.static("uploads/school-logo"));

// For production, serve static files if needed
if (process.env.NODE_ENV === "production") {
  app.use(express.static(path.join(__dirname, "../client/build")));
  app.get("*", (req, res) => {
    res.sendFile(path.join(__dirname, "../client/build", "index.html"));
  });
}

const port = process.env.SERVER_PORT;

async function testConnection() {
  try {
    const connection = await pool.getConnection();
    console.log("Database connected successfully");
    connection.release();
  } catch (err) {
    console.error("Database connection failed:", err);
    process.exit(1);
  }
}

app.listen(port, () => {
  console.log("Server running on port " + port);
  testConnection();
});

module.exports = app;
